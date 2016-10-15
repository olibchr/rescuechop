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

import java.util.*;

public class Flights {
    // Maximum of time between two flight data points in the same flight in seconds
    private static final double MAX_TIME_DELTA = 20 * 60;
    // Minimum duration of a flight in seconds
    private static final double MIN_DURATION = 60;
    // Minimum distance an aircraft should move in a minute in meters
    private static final double MIN_DISTANCE = 10;

    public static void main(String[] args) {
        Logger log = LogManager.getLogger(Flights.class);
        log.setLevel(Level.INFO);

        String inputPath = args[0];
        String outputPath = args[1];

        SparkConf sparkConf = new SparkConf().setAppName("LSDE09 Flights");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        // Load flight data
        final JavaRDD<FlightDatum> flightData = Transformations.readFlightDataCsv(sc, inputPath);
        long recordsCount = flightData.count();

        // Group by aircraft
        JavaPairRDD<String, Iterable<FlightDatum>> flightDataByAircraft = flightData.groupBy(new Function<FlightDatum, String>() {
            public String call(FlightDatum flightDatum) throws Exception {
                return flightDatum.getIcao();
            }
        });
        long aircraftCount = flightDataByAircraft.count();

        // Roughly group flight data into flights
        JavaPairRDD<String, Iterable<Flight>> flightsByAircraft = flightDataByAircraft.mapToPair(new PairFunction<Tuple2<String, Iterable<FlightDatum>>, String, Iterable<Flight>>() {
            public Tuple2<String, Iterable<Flight>> call(Tuple2<String, Iterable<FlightDatum>> tuple) throws Exception {
                String icao = tuple._1;
                List<FlightDatum> flightDataList = Lists.newArrayList(tuple._2);
                Collections.sort(flightDataList);

                List<Flight> flights = new ArrayList<>();

                // First do a rough grouping merely on time
                SortedSet<FlightDatum> lastFlightData = new TreeSet<>();
                double lastTime = flightDataList.get(0).getTime();
                for (FlightDatum fd : flightDataList) {
                    if (fd.getTime() - lastTime >= MAX_TIME_DELTA) {
                        Flight flight = new Flight(icao, lastFlightData);
                        lastFlightData = new TreeSet<>();
                        flights.add(flight);
                    }
                    lastFlightData.add(fd);
                    lastTime = fd.getTime();
                }
                if (!lastFlightData.isEmpty()) {
                    Flight flight = new Flight(icao, lastFlightData);
                    flights.add(flight);
                }

                return new Tuple2<String, Iterable<Flight>>(icao, flights);
            }
        });

        // Flatten
        JavaRDD<Flight> flights = Transformations.flatten(flightsByAircraft).cache();
        long splitTimeCount = flights.count();

        // Filter flights that are too short
        flights = flights.filter(new Function<Flight, Boolean>() {
            public Boolean call(Flight flight) throws Exception {
                return flight.getDuration() > MIN_DURATION;
            }
        }).cache();
        long filterLong1Count = flights.count();

        // Filter flights that do not contain any position data
        flights = flights.filter(new Function<Flight, Boolean>() {
            public Boolean call(Flight flight) throws Exception {
                for (FlightDatum fd : flight.getFlightData()) {
                    if (fd.hasPosition()) {
                        return true;
                    }
                }
                return false;
            }
        }).cache();
        long flightsWithPositionCount = flights.count();

        // Split flights when standing still for a certain time
        flights = flights.flatMap(new FlatMapFunction<Flight, Flight>() {
            public Iterable<Flight> call(Flight flight) throws Exception {
                List<Flight> splitFlights = new ArrayList<>();

                // Put flight data in minute wide bins
                SortedMap<Long, List<FlightDatum>> flightDataPerMinute = new TreeMap<>();
                for (FlightDatum fd : flight.getFlightData()) {
                    long minute = (long) (fd.getTime() / 60);
                    List<FlightDatum> flightData = flightDataPerMinute.get(minute);
                    if (flightData == null) {
                        flightData = new ArrayList<>();
                        flightDataPerMinute.put(minute, flightData);
                    }
                    flightData.add(fd);
                }

                // Calculate distance traveled per minute
                List<Double> splitTimes = new ArrayList<>();
                for (long minute : flightDataPerMinute.keySet()) {
                    boolean split = true;

                    List<FlightDatum> flightData = flightDataPerMinute.get(minute);
                    double startTime = -1;
                    double endTime = -1;
                    for (int i = 0; i < flightData.size() - 1 && split; i++) {
                        Position pos1 = flightData.get(i).getPosition();
                        if (pos1 == null)
                            continue;
                        if (startTime < 0)
                            startTime = flightData.get(i).getTime();

                        for (int j = i + 1; j < flightData.size() && split; j++) {
                            Position pos2 = flightData.get(j).getPosition();
                            if (pos2 == null)
                                continue;
                            endTime = flightData.get(j).getTime();

                            if (pos1.distanceTo(pos2) > MIN_DISTANCE) {
                                split = false;
                            }
                        }
                    }

                    if (split && endTime - startTime > 30) {
                        splitTimes.add(flightData.get(flightData.size() / 2).getTime());
                    }
                }

                // Split flight if we found moments to split on
                if (splitTimes.isEmpty()) {
                    splitFlights.add(flight);
                } else {
                    SortedSet<FlightDatum> lastFlightData = new TreeSet<>();
                    double time = splitTimes.get(0);
                    splitTimes.remove(0);
                    for (FlightDatum fd : flight.getFlightData()) {
                        if (fd.getTime() == time) {
                            splitFlights.add(new Flight(flight.getIcao(), lastFlightData));
                            lastFlightData = new TreeSet<>();
                            if (!splitTimes.isEmpty()) {
                                time = splitTimes.get(0);
                                splitTimes.remove(0);
                            } else {
                                time = Double.MAX_VALUE;
                            }
                        }
                        lastFlightData.add(fd);
                    }
                    splitFlights.add(new Flight(flight.getIcao(), lastFlightData));
                }

                return splitFlights;
            }
        }).cache();
        long splitMovementCount = flights.count();

        // Filter flights that are too short
        flights = flights.filter(new Function<Flight, Boolean>() {
            public Boolean call(Flight flight) throws Exception {
                return flight.getDuration() > MIN_DURATION;
            }
        }).cache();
        long filterLong2Count = flights.count();

        // Write to CSV
        Transformations.saveAsCsv(flights, outputPath);

        // Print statistics
        List<String> statistics = new ArrayList<String>();
        statistics.add(numberOfItemsStatistic("input records", recordsCount));
        statistics.add(numberOfItemsStatistic("input aircraft", aircraftCount));
        statistics.add(numberOfItemsStatistic("flights after splitting on time only            ", splitTimeCount));
        statistics.add(numberOfItemsStatistic("flights after filtering on time                 ", filterLong1Count));
        statistics.add(numberOfItemsStatistic("flights after filtering on having location data ", flightsWithPositionCount));
        statistics.add(numberOfItemsStatistic("flights after splitting on distance traveled    ", splitMovementCount));
        statistics.add(numberOfItemsStatistic("flights after filtering on time                 ", filterLong2Count));
        saveStatisticsAsTextFile(sc, outputPath, statistics);
    }

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