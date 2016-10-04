package vu.lsde.sampler;

import org.apache.avro.generic.GenericRecord;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.opensky.libadsb.msgs.AirbornePositionMsg;
import org.opensky.libadsb.msgs.AirspeedHeadingMsg;
import org.opensky.libadsb.msgs.AltitudeReply;
import org.opensky.libadsb.msgs.VelocityOverGroundMsg;
import scala.Tuple2;
import vu.lsde.core.Config;
import vu.lsde.core.io.SparkAvroReader;
import vu.lsde.core.model.SensorDatum;

import java.io.IOException;

public class Sampler {

    public static void main(String[] args) throws IOException {
//        Logger log = LogManager.getLogger(Sampler.class);
//        log.setLevel(Level.INFO);

        String inputPath = Config.OPEN_SKY_DATA_PATH; // + "raw2015092100.avro";
//        String outputPath = Config.OPEN_SKY_SAMPLE_DATA_PATH;
        String outputPath = args[0];

        SparkConf sparkConf = new SparkConf().setAppName("LSDE09 Sampler");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);

//        log.info("Loading AVRO to RDD");

        // Load records
        JavaRDD<GenericRecord> records = SparkAvroReader.loadJavaRDD(sc, inputPath, Config.OPEN_SKY_SCHEMA);

//        log.info("Mapping GenericRecord objects to SensorDatum objects");

        // Map to model
        JavaRDD<SensorDatum> sensorData = records.map(new Function<GenericRecord, SensorDatum>() {
            public SensorDatum call(GenericRecord genericRecord) throws Exception {
                return SensorDatum.fromGenericRecord(genericRecord);
            }
        });

//        log.info("Filtering out invalid SensorDatum objects");

        // Filter out invalid messages
        sensorData = sensorData.filter(new Function<SensorDatum, Boolean>() {
            public Boolean call(SensorDatum sensorDatum) throws Exception {
                return sensorDatum.isValidMessage();
            }
        });

//        log.info("Grouping SensorDatum objects by icao");

        // Group models by icao
        JavaPairRDD<String, Iterable<SensorDatum>> sensorDataByAircraft = sensorData.groupBy(new Function<SensorDatum, String>() {
            public String call(SensorDatum sensorDatum) {
                return sensorDatum.icao;
            }
        });

//        log.info("Filtering out aircraft flying above 3km");

        // Find aircraft flying lower than 3km
        JavaPairRDD<String, Iterable<SensorDatum>> possibleHelicopters = sensorDataByAircraft.filter(new Function<Tuple2<String, Iterable<SensorDatum>>, Boolean>() {
            public Boolean call(Tuple2<String, Iterable<SensorDatum>> tuple) throws Exception {
                for (SensorDatum sd: tuple._2) {
                    if (sd.decodedMessage instanceof AltitudeReply) {
                        if (((AltitudeReply) sd.decodedMessage).getAltitude() > 3000) {
                            return false;
                        }
                    } else if (sd.decodedMessage instanceof AirbornePositionMsg) {
                        AirbornePositionMsg msg = (AirbornePositionMsg) sd.decodedMessage;
                        if (msg.hasAltitude() && msg.getAltitude() > 3000) {
                            return false;
                        }
                    } else if (sd.decodedMessage instanceof AirspeedHeadingMsg) {
                        AirspeedHeadingMsg msg = (AirspeedHeadingMsg) sd.decodedMessage;
                        if (msg.hasAirspeedInfo() && msg.getAirspeed() > 120) {
                            return false;
                        }
                    } else if (sd.decodedMessage instanceof VelocityOverGroundMsg) {
                        VelocityOverGroundMsg msg = (VelocityOverGroundMsg) sd.decodedMessage;
                        if (msg.hasVelocityInfo() && msg.getVelocity() > 120) {
                            return false;
                        }
                    }
                }
                return true;
            }
        });

//        log.info("Flattening SensorDatum objects");

        // Flatten
        JavaRDD<SensorDatum> sample = possibleHelicopters.values().flatMap(new FlatMapFunction<Iterable<SensorDatum>, SensorDatum>() {
            public Iterable<SensorDatum> call(Iterable<SensorDatum> sensorData) throws Exception {
                return sensorData;
            }
        });

//        log.info("Mapping SensorDatum objects to CSV lines");

        // To CSV
        JavaRDD<String> sampleCSV = sample.map(new Function<SensorDatum, String>() {
            public String call(SensorDatum sensorDatum) throws Exception {
                return sensorDatum.toCSV();
            }
        });

//        log.info("Saving CSV lines as text file");

        sampleCSV.saveAsTextFile(outputPath);
    }
}
