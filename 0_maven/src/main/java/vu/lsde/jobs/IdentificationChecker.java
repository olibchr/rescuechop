package vu.lsde.jobs;

import com.clearspring.analytics.util.Lists;
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
import org.apache.spark.api.java.function.PairFunction;
import org.opensky.libadsb.Position;
import org.opensky.libadsb.PositionDecoder;
import org.opensky.libadsb.msgs.AirbornePositionMsg;
import org.opensky.libadsb.msgs.IdentificationMsg;
import org.opensky.libadsb.msgs.ModeSReply;
import org.opensky.libadsb.msgs.SurfacePositionMsg;
import scala.Tuple2;
import vu.lsde.core.Config;
import vu.lsde.core.io.SparkAvroReader;
import vu.lsde.core.model.AircraftPosition;
import vu.lsde.core.model.SensorDatum;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

/**
 * Job that takes sensor data as input, groups it by ICAO, and checks each aircraft for ADS-B identifcation messages.
 * Only sensor data that belongs to aircraft that specifically state they are a rotorcraft is left in.
 */
public class IdentificationChecker {

    public static void main(String[] args) throws IOException {
        Logger log = LogManager.getLogger(IdentificationChecker.class);
        log.setLevel(Level.INFO);

        String inputPath = args[0];
        String outputPath = args[1];

        SparkConf sparkConf = new SparkConf().setAppName("LSDE09 IdentificationChecker");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        // Load records
        JavaRDD<GenericRecord> records = SparkAvroReader.loadJavaRDD(sc, inputPath, Config.OPEN_SKY_SCHEMA);
        long recordsCount = records.count();

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
        long validRecordsCount = sensorData.count();

        // Group models by icao
        JavaPairRDD<String, Iterable<SensorDatum>> sensorDataByAircraft = sensorData.groupBy(new Function<SensorDatum, String>() {
            public String call(SensorDatum sensorDatum) {
                return sensorDatum.getIcao();
            }
        });
        long aircraftCount = sensorDataByAircraft.count();

        // Filter out all aircraft that are explicitly rotorcrafts
        sensorDataByAircraft = sensorDataByAircraft.filter(new Function<Tuple2<String, Iterable<SensorDatum>>, Boolean>() {
            public Boolean call(Tuple2<String, Iterable<SensorDatum>> tuple) throws Exception {
                for (SensorDatum sd: tuple._2) {
                    if (sd.getDecodedMessage() instanceof IdentificationMsg) {
                        IdentificationMsg msg = (IdentificationMsg) sd.getDecodedMessage();
                        return msg.getCategoryDescription().equals("Rotorcraft");
                    }
                }
                return false;
            }
        });
        long rotorcraftCount = sensorDataByAircraft.count();

        // Flatten
        sensorData = sensorDataByAircraft.flatMap(new FlatMapFunction<Tuple2<String, Iterable<SensorDatum>>, SensorDatum>() {
            public Iterable<SensorDatum> call(Tuple2<String, Iterable<SensorDatum>> tuple) throws Exception {
                return tuple._2;
            }
        });

        // To CSV
        JavaRDD<String> sensorDataCSV = sensorData.map(new Function<SensorDatum, String>() {
            public String call(SensorDatum sensorDatum) throws Exception {
                return sensorDatum.toCSV();
            }
        });
        long outputLinesCount = sensorDataCSV.count();

        // To file
        sensorDataCSV.saveAsTextFile(outputPath);

        // Print statistics
        List<String> statistics = new ArrayList<String>();
        statistics.add(numberOfItemsStatistic("raw records", recordsCount));
        statistics.add(numberOfItemsStatistic("valid records", validRecordsCount));
        statistics.add(numberOfItemsStatistic("unique aircraft", aircraftCount));
        statistics.add(numberOfItemsStatistic("definite helicopters", rotorcraftCount));
        statistics.add(numberOfItemsStatistic("messages in final sample", outputLinesCount));
        saveStatisticsAsTextFile(sc, outputPath, statistics);
    }

    private static String numberOfItemsStatistic(String itemName, long count) {
        return String.format("Number of %s: %d", itemName, count);
    }

    private static void saveStatisticsAsTextFile(JavaSparkContext sc, String outputPath, List<String> statisticsLines) {
        JavaRDD<String> statsRDD = sc.parallelize(statisticsLines).coalesce(1);
        statsRDD.saveAsTextFile(outputPath + "_stats");
    }
}
