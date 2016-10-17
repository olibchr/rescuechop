package vu.lsde.jobs;

import org.apache.avro.generic.GenericRecord;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.opensky.libadsb.msgs.IdentificationMsg;
import org.opensky.libadsb.msgs.ModeSReply;
import scala.Tuple2;
import vu.lsde.core.Config;
import vu.lsde.core.io.SparkAvroReader;
import vu.lsde.core.model.SensorDatum;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Takes a avro files containing sensor data as input, and then creates two sets of data: one with definite airplanes and
 * one with definite helicopters. These can then be used to evaluate our classifiers.
 */
public class EvaluationSampler extends JobBase {

    public static void main(String[] args) throws IOException {
        String inputPath = args[0];
        String outputPath = args[1];

        SparkConf sparkConf = new SparkConf().setAppName("LSDE09 Sampler")
                .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                .registerKryoClasses(new Class[]{SensorDatum.class});
//                .set("spark.core.connection.ack.wait.timeout", "600s");

        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        // Load records
        JavaRDD<GenericRecord> records = SparkAvroReader.loadJavaRDD(sc, inputPath, Config.OPEN_SKY_SCHEMA);

        // Map to model
        JavaRDD<SensorDatum> sensorData = records.map(new Function<GenericRecord, SensorDatum>() {
            public SensorDatum call(GenericRecord genericRecord) throws Exception {
                return SensorDatum.fromGenericRecord(genericRecord);
            }
        });
        long inputRecordsCount = sensorData.count();

        // Filter out invalid messages
        sensorData = sensorData.filter(new Function<SensorDatum, Boolean>() {
            public Boolean call(SensorDatum sensorDatum) throws Exception {
                return sensorDatum.isValidMessage();
            }
        });
        long validRecordsCount = sensorData.count();

        // Filter out messages we won't use anyway
        sensorData = sensorData.filter(new Function<SensorDatum, Boolean>() {
            public Boolean call(SensorDatum sensorDatum) throws Exception {
                switch(sensorDatum.getDecodedMessage().getType()) {
                    case MODES_REPLY:
                    case SHORT_ACAS:
                    case LONG_ACAS:
                    case EXTENDED_SQUITTER:
                    case COMM_D_ELM:
                    case ADSB_EMERGENCY:
                    case ADSB_TCAS:
                        return false;
                }
                return true;
            }
        });

        JavaPairRDD<String, Iterable<SensorDatum>> sensorDataByAircraft = sensorData.groupBy(new Function<SensorDatum, String>() {
            @Override
            public String call(SensorDatum sensorDatum) throws Exception {
                return sensorDatum.getIcao();
            }
        });
        long aircraftCount = sensorDataByAircraft.count();

        JavaPairRDD<String, Iterable<SensorDatum>> sensorDataByHelis = sensorDataByAircraft.filter(new Function<Tuple2<String, Iterable<SensorDatum>>, Boolean>() {
            @Override
            public Boolean call(Tuple2<String, Iterable<SensorDatum>> t) throws Exception {
                for (SensorDatum sd : t._2) {
                    ModeSReply msg = sd.getDecodedMessage();
                    if (msg instanceof IdentificationMsg) {
                        IdentificationMsg idMsg = (IdentificationMsg) msg;
                        return idMsg.getCategoryDescription().equals("Rotorcraft");
                    }
                }
                return false;
            }
        });
        long heliCount = sensorDataByHelis.count();

        JavaPairRDD<String, Iterable<SensorDatum>> sensorDataByAirplanes = sensorDataByAircraft.filter(new Function<Tuple2<String, Iterable<SensorDatum>>, Boolean>() {
            @Override
            public Boolean call(Tuple2<String, Iterable<SensorDatum>> t) throws Exception {
                for (SensorDatum sd : t._2) {
                    ModeSReply msg = sd.getDecodedMessage();
                    if (msg instanceof IdentificationMsg) {
                        IdentificationMsg idMsg = (IdentificationMsg) msg;
                        return idMsg.getEmitterCategory() != 0 && !idMsg.getCategoryDescription().equals("Rotorcraft");
                    }
                }
                return false;
            }
        });
        long planeCount = sensorDataByAircraft.count();

        // Flatten
        JavaRDD<SensorDatum> helis = flatten(sensorDataByHelis);
        long outputHeliDataCount = helis.count();

        JavaRDD<SensorDatum> airplanes = flatten(sensorDataByAirplanes);
        long outputAirplaneDataCount = airplanes.count();

        // To CSV
        saveAsCsv(helis, outputPath + "_helis");
        saveAsCsv(airplanes, outputPath + "_planes");

        // Print statistics
        List<String> statistics = new ArrayList<>();
        statistics.add(numberOfItemsStatistic("raw records              ", inputRecordsCount));
        statistics.add(numberOfItemsStatistic("valid records            ", validRecordsCount));
        statistics.add(numberOfItemsStatistic("unique aircraft          ", aircraftCount));
        statistics.add(numberOfItemsStatistic("helicopters              ", heliCount));
        statistics.add(numberOfItemsStatistic("airplanes                ", planeCount));
        statistics.add(numberOfItemsStatistic("messages in heli sample  ", outputHeliDataCount));
        statistics.add(numberOfItemsStatistic("messages in plane sample ", outputAirplaneDataCount));
        saveStatisticsAsTextFile(sc, outputPath, statistics);
    }
}
