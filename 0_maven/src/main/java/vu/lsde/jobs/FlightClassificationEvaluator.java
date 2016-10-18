package vu.lsde.jobs;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.opensky.libadsb.Position;
import vu.lsde.core.model.Flight;
import vu.lsde.core.services.HelipadPositionsService;

import java.util.ArrayList;
import java.util.List;

import static vu.lsde.jobs.functions.ClassifierFunctions.classifyHelicoperFlights;

public class FlightClassificationEvaluator extends JobBase {
    public static void main(String[] args) {
        String inputPath = args[0];
        String outputPath = args[1];

        SparkConf sparkConf = new SparkConf().setAppName("LSDE09 FlightClassificationEvaluator");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        // Get datasets
        JavaRDD<Flight> heliFlights = readFlightsCsv(sc, inputPath + "_heli_flights");
        JavaRDD<Flight> planeFlights = readFlightsCsv(sc, inputPath + "_plane_flights");

        // Group by icao
        JavaPairRDD<String, Iterable<Flight>> heliFlightsByHeli = groupByIcao(heliFlights);
        JavaPairRDD<String, Iterable<Flight>> planeFlightsByPlane = groupByIcao(planeFlights);

        // Count amount of aircraft
        long heliCount = heliFlightsByHeli.keys().distinct().count();
        long planeCount = planeFlightsByPlane.keys().distinct().count();

        // Classify
        final Broadcast<List<Position>> helipads = sc.broadcast(HelipadPositionsService.getHelipadPositions(sc));
        JavaPairRDD<String, Iterable<Flight>> truePositives = heliFlightsByHeli.filter(classifyHelicoperFlights(helipads));
        JavaPairRDD<String, Iterable<Flight>> falsePositives = planeFlightsByPlane.filter(classifyHelicoperFlights(helipads));

        long truePositivesCount = truePositives.keys().distinct().count();
        long falsePositivesCount = falsePositives.keys().distinct().count();
        long falseNegativesCount = heliCount - truePositivesCount;
        long trueNegativesCount = planeCount - falsePositivesCount;

        // Create stats
        List<String> statistics = new ArrayList<>();
        statistics.add(numberOfItemsStatistic("True positives ", truePositivesCount, heliCount));
        statistics.add(numberOfItemsStatistic("False negatives", falseNegativesCount, heliCount));
        statistics.add(numberOfItemsStatistic("False positives", falsePositivesCount, planeCount));
        statistics.add(numberOfItemsStatistic("True negatives ", trueNegativesCount, planeCount));
        saveStatisticsAsTextFile(sc, outputPath, statistics);
    }
}
