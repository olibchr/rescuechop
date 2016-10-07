package vu.lsde.jobs;

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
import org.opensky.libadsb.msgs.AirbornePositionMsg;
import org.opensky.libadsb.msgs.AirspeedHeadingMsg;
import org.opensky.libadsb.msgs.AltitudeReply;
import org.opensky.libadsb.msgs.VelocityOverGroundMsg;
import scala.Tuple2;
import vu.lsde.core.Config;
import vu.lsde.core.io.SparkAvroReader;
import vu.lsde.core.model.SensorDatum;

import java.io.IOException;

/**
 * Takes a avro files containing sensor data as input, and then filters out any sensor data belonging to aircraft that
 * are definitely NOT rotorcraft. This is done by looking at speed and altitude. The remaining sensordata is then output
 * in the form of CSV.
 */
public class Sampler {

    public static void main(String[] args) throws IOException {
        Logger log = LogManager.getLogger(Sampler.class);
        log.setLevel(Level.INFO);

        String inputPath = args[0]; //Config.OPEN_SKY_DATA_PATH;// + "raw2015090800.avro";
        String outputPath = args[1];
//        String outputPath = Config.OPEN_SKY_SAMPLE_DATA_PATH;

        SparkConf sparkConf = new SparkConf().setAppName("LSDE09 Sampler");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        // Load records
        JavaRDD<GenericRecord> records = SparkAvroReader.loadJavaRDD(sc, inputPath, Config.OPEN_SKY_SCHEMA);

        // Map to model
        JavaRDD<SensorDatum> sensorData = records.map(new Function<GenericRecord, SensorDatum>() {
            public SensorDatum call(GenericRecord genericRecord) throws Exception {
                return SensorDatum.fromGenericRecord(genericRecord);
            }
        });

        // Filter out invalid messages
        sensorData = sensorData.filter(new Function<SensorDatum, Boolean>() {
            public Boolean call(SensorDatum sensorDatum) throws Exception {
                return sensorDatum.isValidMessage();
            }
        });

        // Group models by icao
        JavaPairRDD<String, Iterable<SensorDatum>> sensorDataByAircraft = sensorData.groupBy(new Function<SensorDatum, String>() {
            public String call(SensorDatum sensorDatum) {
                return sensorDatum.getIcao();
            }
        });

        // Find aircraft flying lower than 3km
        JavaPairRDD<String, Iterable<SensorDatum>> possibleHelicopters = sensorDataByAircraft.filter(new Function<Tuple2<String, Iterable<SensorDatum>>, Boolean>() {
            public Boolean call(Tuple2<String, Iterable<SensorDatum>> tuple) throws Exception {
                for (SensorDatum sd: tuple._2) {
                    if (sd.getDecodedMessage() instanceof AltitudeReply) {
                        if (((AltitudeReply) sd.getDecodedMessage()).getAltitude() > 3000) {
                            return false;
                        }
                    } else if (sd.getDecodedMessage() instanceof AirbornePositionMsg) {
                        AirbornePositionMsg msg = (AirbornePositionMsg) sd.getDecodedMessage();
                        if (msg.hasAltitude() && msg.getAltitude() > 3000) {
                            return false;
                        }
                    } else if (sd.getDecodedMessage() instanceof AirspeedHeadingMsg) {
                        AirspeedHeadingMsg msg = (AirspeedHeadingMsg) sd.getDecodedMessage();
                        if (msg.hasAirspeedInfo() && msg.getAirspeed() > 120) {
                            return false;
                        }
                    } else if (sd.getDecodedMessage() instanceof VelocityOverGroundMsg) {
                        VelocityOverGroundMsg msg = (VelocityOverGroundMsg) sd.getDecodedMessage();
                        if (msg.hasVelocityInfo() && msg.getVelocity() > 120) {
                            return false;
                        }
                    }
                }
                return true;
            }
        });

        // Flatten
        JavaRDD<SensorDatum> sample = possibleHelicopters.values().flatMap(new FlatMapFunction<Iterable<SensorDatum>, SensorDatum>() {
            public Iterable<SensorDatum> call(Iterable<SensorDatum> sensorData) throws Exception {
                return sensorData;
            }
        });

        // To CSV
        JavaRDD<String> sampleCSV = sample.map(new Function<SensorDatum, String>() {
            public String call(SensorDatum sensorDatum) throws Exception {
                return sensorDatum.toCSV();
            }
        });

        sampleCSV.saveAsTextFile(outputPath);
    }
}
