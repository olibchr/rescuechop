package vu.lsde.jobs;

import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import vu.lsde.core.model.Flight;
import vu.lsde.jobs.functions.ClassifierFunctions;

public class FlightsAnalyzer extends JobBase {
    public static void main(String[] args) {
        Logger log = LogManager.getLogger(Flights.class);
        log.setLevel(Level.INFO);

        String inputPath = args[0];
        String outputPath = args[1];

        SparkConf sparkConf = new SparkConf().setAppName("LSDE09 FlightsAnalyzer");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        // Get flights
        JavaRDD<Flight> flights = readFlightsCsv(sc, inputPath);

        // Group by aircraft
        JavaPairRDD<String, Iterable<Flight>> flightsByAircraft = groupByIcao(flights);

        // Filter helicopters
        JavaPairRDD<String, Iterable<Flight>> flightsByHelicopters = flightsByAircraft.filter(ClassifierFunctions.classifyHelicopterFlights);

        // Find heli icao's
        JavaRDD<String> helicopters = flightsByHelicopters.keys();

        // Find heli flights
        flights = flatten(flightsByHelicopters);

        // Save
        saveAsCsv(flights, outputPath + "_flights");
        helicopters.saveAsTextFile(outputPath + "_icaos");
    }




}
