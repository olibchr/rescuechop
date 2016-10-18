package vu.lsde.jobs;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;
import vu.lsde.core.model.FlightDatum;
import vu.lsde.core.util.Grouping;

import java.util.ArrayList;
import java.util.List;
import java.util.SortedMap;

import static vu.lsde.jobs.functions.FlightDataFunctions.mergeFlightData;

public class MergeFlightData extends JobBase {

    public static void main(String[] args) {
        String inputPath = args[0];
        String outputPath = args[1];

        SparkConf sparkConf = new SparkConf().setAppName("LSDE09 MergeFlightData");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        // Load flight data
        JavaRDD<FlightDatum> flightData = readFlightDataCsv(sc, inputPath);
        long recordsCount = flightData.count();

        // Group by aircraft
        JavaPairRDD<String, Iterable<FlightDatum>> flightDataByAircraft = groupByIcao(flightData);

        // Merge flight data that are from the same timestamp
        JavaPairRDD<String, Iterable<FlightDatum>> mergedFlightDataByAircraft = flightDataByAircraft.mapToPair(mergeFlightData());

        // Flatten
        flightData = flatten(mergedFlightDataByAircraft).cache();

        // Write to CSV
        saveAsCsv(flightData, outputPath);

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

        // Print statistics
        List<String> statistics = new ArrayList<>();
        statistics.add(numberOfItemsStatistic("input records     ", recordsCount));
        statistics.add(numberOfItemsStatistic("output flight data", flightDataCount));
        statistics.add(numberOfItemsStatistic("position data     ", positionDataCount, flightDataCount));
        statistics.add(numberOfItemsStatistic("altitude data     ", altitudeDataCount, flightDataCount));
        statistics.add(numberOfItemsStatistic("velocity data     ", velocityDataCount, flightDataCount));
        statistics.add(numberOfItemsStatistic("rate of climb data", rocDataCount, flightDataCount));
        saveStatisticsAsTextFile(sc, outputPath, statistics);
    }
}
