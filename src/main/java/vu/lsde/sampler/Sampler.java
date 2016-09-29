package vu.lsde.sampler;

import org.apache.avro.generic.GenericRecord;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.opensky.libadsb.msgs.AirbornePositionMsg;
import org.opensky.libadsb.msgs.AltitudeReply;
import scala.Tuple2;
import vu.lsde.core.Config;
import vu.lsde.core.io.SparkAvroReader;
import vu.lsde.core.model.SensorDatum;

import java.io.IOException;

public class Sampler {

    public static void main(String[] args) throws IOException {
        if (args.length == 0) throw new IllegalArgumentException("Expected an output argument");

        String inputPath = Config.OPEN_SKY_DATA_PATH;
        String outputPath = Config.OPEN_SKY_SAMPLE_DATA_PATH;

        SparkConf sparkConf = new SparkConf().setAppName("LSDE09 Sampler");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        // Load records
        JavaRDD<GenericRecord> records = SparkAvroReader.loadJavaRDD(sc, inputPath, Config.OPEN_SKY_SCHEMA);

        // Map to model
        final JavaRDD<SensorDatum> sensorData = records.map(new Function<GenericRecord, SensorDatum>() {
            public SensorDatum call(GenericRecord genericRecord) throws Exception {
                return SensorDatum.fromGenericRecord(genericRecord);
            }
        });

        // Group models by icao
        JavaPairRDD<String, Iterable<SensorDatum>> sensorDataByAircraft = sensorData.mapToPair(new PairFunction<SensorDatum, String, SensorDatum>() {
            public Tuple2<String, SensorDatum> call(SensorDatum sensorDatum) throws Exception {
                return new Tuple2<String, SensorDatum>(sensorDatum.icao, sensorDatum);
            }
        }).groupByKey().cache();

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
