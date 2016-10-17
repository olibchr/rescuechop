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
import org.opensky.libadsb.Position;
import scala.Tuple2;
import vu.lsde.core.model.Flight;
import vu.lsde.core.model.FlightDatum;
import vu.lsde.core.util.Geo;
import vu.lsde.core.util.Grouping;
import vu.lsde.jobs.functions.FlightFunctions;

import java.util.*;

public class Flights extends JobBase {
    // Maximum of time between two flight data points in the same flight in seconds
    private static final double MAX_TIME_DELTA = 20 * 60;
    // Minimum duration of a flight in seconds
    private static final double MIN_DURATION = 2 * 60;
    // Minimum distance an aircraft should move in a minute in meters
    private static final double MIN_DISTANCE = 8;
    // Minimum altitude for an aircraft to consider it flying
    private static final double MIN_ALTITUDE = 30;

    public static void main(String[] args) {
        Logger log = LogManager.getLogger(Flights.class);
        log.setLevel(Level.INFO);

        String inputPath = args[0];
        String outputPath = args[1];

        SparkConf sparkConf = new SparkConf().setAppName("LSDE09 Flights");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        // Load flight data
        JavaRDD<FlightDatum> flightData = readFlightDataCsv(sc, inputPath);
        long recordsCount = flightData.count();

        // Group by aircraft
        JavaPairRDD<String, Iterable<FlightDatum>> flightDataByAircraft = groupByIcao(flightData);
        long aircraftCount = flightDataByAircraft.count();

        // Roughly group flight data into flights
        JavaPairRDD<String, Iterable<Flight>> flightsByAircraft = flightDataByAircraft.mapToPair(FlightFunctions.splitFlightDataToFlightsOnTime);

        // Flatten
        JavaRDD<Flight> flights = flatten(flightsByAircraft).cache();
        long splitTimeCount = flights.count();

        // Filter flights that are too short
        flights = flights.filter(FlightFunctions.isLongFlight).cache();
        long filterLong1Count = flights.count();

        // Filter flights that do not contain any position data
        flights = flights.filter(FlightFunctions.hasPositionData).cache();
        long flightsWithPositionCount = flights.count();

        // Split flights on altitude, or position if that's not possible
        flights = flights.flatMap(FlightFunctions.splitFlightsOnAltitudeOrDistance).cache();
        long splitMovementCount = flights.count();

        // Filter flights that are too short
        flights = flights.filter(FlightFunctions.isLongFlight).cache();
        long filterLong2Count = flights.count();

        long outputAircraftCount = groupByIcao(flights).count();

        // Write to CSV
        saveAsCsv(flights, outputPath);

        // Print statistics
        List<String> statistics = new ArrayList<String>();
        statistics.add(numberOfItemsStatistic("input records", recordsCount));
        statistics.add(numberOfItemsStatistic("input aircraft", aircraftCount));
        statistics.add(numberOfItemsStatistic("flights after splitting on time only            ", splitTimeCount));
        statistics.add(numberOfItemsStatistic("flights after filtering on time                 ", filterLong1Count));
        statistics.add(numberOfItemsStatistic("flights after filtering on having location data ", flightsWithPositionCount));
        statistics.add(numberOfItemsStatistic("flights after splitting on altitude/position    ", splitMovementCount));
        statistics.add(numberOfItemsStatistic("flights after filtering on time                 ", filterLong2Count));
        statistics.add(numberOfItemsStatistic("output aircraft", outputAircraftCount));
        saveStatisticsAsTextFile(sc, outputPath, statistics);
    }
}