package vu.lsde.jobs;

import com.clearspring.analytics.util.Lists;
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
import scala.Tuple2;
import vu.lsde.core.model.FlightDatum;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class MergeFlightData {

    public static void main(String[] args) {
        String inputPath = args[0];
        String outputPath = args[1];

        SparkConf sparkConf = new SparkConf().setAppName("LSDE09 MergeFlightData");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        // Load flight data
        JavaRDD<FlightDatum> flightData = Transformations.readFlightDataCsv(sc, inputPath);
        long recordsCount = flightData.count();

        // Group by aircraft
        JavaPairRDD<String, Iterable<FlightDatum>> flightDataByAircraft = flightData.groupBy(new Function<FlightDatum, String>() {
            public String call(FlightDatum flightDatum) throws Exception {
                return flightDatum.getIcao();
            }
        });

        // Merge flight data that are from the same timestamp
        JavaPairRDD<String, Iterable<FlightDatum>> mergedFlightDataByAircraft = flightDataByAircraft.mapToPair(new PairFunction<Tuple2<String, Iterable<FlightDatum>>, String, Iterable<FlightDatum>>() {
            public Tuple2<String, Iterable<FlightDatum>> call(Tuple2<String, Iterable<FlightDatum>> tuple) throws Exception {
                String icao = tuple._1;
                Iterable<FlightDatum> flightData = tuple._2;

                List<FlightDatum> mergedFlightData = mergeFlightData(flightData);

                return new Tuple2<String, Iterable<FlightDatum>>(icao, mergedFlightData);
            }
        });

        // Flatten
        flightData = Transformations.flatten(mergedFlightDataByAircraft).cache();

        // Write to CSV
        Transformations.saveAsCsv(flightData, outputPath);

        // Get statistics on flight data
        long flightDataCount = flightData.count();
        long positionDataCount = flightData.filter(new Function<FlightDatum, Boolean>() {
            public Boolean call(FlightDatum flightDatum) throws Exception {
                return flightDatum.getLatitude() != null;
            }
        }).count();
        long altitudeDataCount = flightData.filter(new Function<FlightDatum, Boolean>() {
            public Boolean call(FlightDatum flightDatum) throws Exception {
                return flightDatum.getAltitude() != null;
            }
        }).count();
        long velocityDataCount = flightData.filter(new Function<FlightDatum, Boolean>() {
            public Boolean call(FlightDatum flightDatum) throws Exception {
                return flightDatum.getVelocity() != null;
            }
        }).count();
        long rocDataCount = flightData.filter(new Function<FlightDatum, Boolean>() {
            public Boolean call(FlightDatum flightDatum) throws Exception {
                return flightDatum.getRateOfClimb() != null;
            }
        }).count();
        long headingDataCount = flightData.filter(new Function<FlightDatum, Boolean>() {
            public Boolean call(FlightDatum flightDatum) throws Exception {
                return flightDatum.getHeading() != null;
            }
        }).count();

        // Print statistics
        List<String> statistics = new ArrayList<String>();
        statistics.add(numberOfItemsStatistic("input records     ", recordsCount));
        statistics.add(numberOfItemsStatistic("output flight data", flightDataCount));
        statistics.add(numberOfItemsStatistic("position data     ", positionDataCount, flightDataCount));
        statistics.add(numberOfItemsStatistic("altitude data     ", altitudeDataCount, flightDataCount));
        statistics.add(numberOfItemsStatistic("velocity data     ", velocityDataCount, flightDataCount));
        statistics.add(numberOfItemsStatistic("rate of climb data", rocDataCount, flightDataCount));
        statistics.add(numberOfItemsStatistic("heading data      ", headingDataCount, flightDataCount));
        saveStatisticsAsTextFile(sc, outputPath, statistics);
    }

    protected static List<FlightDatum> mergeFlightData(Iterable<FlightDatum> flightData) {
        List<FlightDatum> result = new ArrayList<FlightDatum>();

        // Map to list and sort
        List<FlightDatum> flightDataList = Lists.newArrayList(flightData);
        Collections.sort(flightDataList);

        FlightDatum mergedFlightDatum = flightDataList.get(0);
        for (FlightDatum fd : flightDataList) {
            if (fd.getTime() < mergedFlightDatum.getTime() + 1) {
                mergedFlightDatum = mergedFlightDatum.extend(fd);
            } else {
                result.add(mergedFlightDatum);
                mergedFlightDatum = fd;
            }
        }
        result.add(mergedFlightDatum);

        return result;
    }

    // Helper methods

    private static String numberOfItemsStatistic(String itemName, long count) {
        return String.format("Number of %s: %d", itemName, count);
    }

    private static String numberOfItemsStatistic(String itemName, long count, long parentCount) {
        return String.format("Number of %s: %d (%.2f%%)", itemName, count, 100.0 * count / parentCount);
    }

    private static void saveStatisticsAsTextFile(JavaSparkContext sc, String outputPath, List<String> statisticsLines) {
        JavaRDD<String> statsRDD = sc.parallelize(statisticsLines).coalesce(1);
        statsRDD.saveAsTextFile(outputPath + "_stats");
    }
}
