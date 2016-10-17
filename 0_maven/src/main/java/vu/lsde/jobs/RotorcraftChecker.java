package vu.lsde.jobs;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.opensky.libadsb.msgs.IdentificationMsg;
import scala.Tuple2;
import vu.lsde.core.model.SensorDatum;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Job that takes sensor data as input, groups it by ICAO, and checks each aircraft for ADS-B identifcation messages.
 * Only sensor data that belongs to aircraft that specifically state they are a rotorcraft is left in.
 */
public class RotorcraftChecker extends JobBase {

    public static void main(String[] args) throws IOException {
        String inputPath = args[0];
        String outputPath = args[1];

        SparkConf sparkConf = new SparkConf().setAppName("LSDE09 RotorcraftChecker");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        // Map to model
        JavaRDD<SensorDatum> sensorData = readSensorDataCsv(sc, inputPath);
        long recordsCount = sensorData.count();

        // Filter out invalid messages
        sensorData = sensorData.filter(new Function<SensorDatum, Boolean>() {
            public Boolean call(SensorDatum sensorDatum) throws Exception {
                return sensorDatum.isValidMessage() || sensorDatum.getDecodedMessage() instanceof IdentificationMsg;
            }
        });
        long usefulRecordsCount = sensorData.count();

        // Group models by icao
        JavaPairRDD<String, Iterable<SensorDatum>> sensorDataByAircraft = groupByIcao(sensorData);

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

        // Print statistics
        List<String> statistics = new ArrayList<String>();
        statistics.add(numberOfItemsStatistic("raw records", recordsCount));
        statistics.add(numberOfItemsStatistic("valid records", usefulRecordsCount));
        statistics.add(numberOfItemsStatistic("definite helicopters", rotorcraftCount));
        saveStatisticsAsTextFile(sc, outputPath, statistics);
    }
}
