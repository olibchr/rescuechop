package vu.lsde.jobs;

import com.clearspring.analytics.util.Lists;
import com.goebl.simplify.Simplify;
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
import vu.lsde.core.util.Util;

import java.io.Serializable;
import java.util.*;

public class Flights {
    private static final double MIN_TIME_DELTA = 20 * 60;
    private static final double MIN_DURATION = 60;
    private static final double MIN_DISTANCE = 10;

    public static void main(String[] args) {
        Logger log = LogManager.getLogger(Flights.class);
        log.setLevel(Level.INFO);

        String inputPath = args[0];
        String outputPath = args[1];

        SparkConf sparkConf = new SparkConf().setAppName("LSDE09 Flights");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        // Load CSV
        JavaRDD<String> records = sc.textFile(inputPath);
        long recordsCount = records.count();

        // Map to model
        final JavaRDD<FlightDatum> flightData = records.map(new Function<String, FlightDatum>() {
            public FlightDatum call(String s) throws Exception {
                return FlightDatum.fromCSV(s);
            }
        });

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

                List<Flight> flights = new ArrayList<Flight>();

                // First do a rough grouping merely on time
                List<FlightDatum> lastFlightData = new ArrayList<FlightDatum>();
                double lastTime = flightDataList.get(0).getTime();
                for (FlightDatum fd : flightDataList) {
                    if (fd.getTime() - lastTime >= MIN_TIME_DELTA) {
                        Flight flight = new Flight(icao, lastFlightData);
                        lastFlightData.clear();
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
        JavaRDD<Flight> flights = flightsByAircraft.flatMap(new FlatMapFunction<Tuple2<String, Iterable<Flight>>, Flight>() {
            public Iterable<Flight> call(Tuple2<String, Iterable<Flight>> tuple) throws Exception {
                return tuple._2;
            }
        }).cache();
        long initialFlightsCount = flights.count();

        // Filter flights that are too short
        JavaRDD<Flight> longFlights = flights.filter(new Function<Flight, Boolean>() {
            public Boolean call(Flight flight) throws Exception {
                return flight.getDuration() > MIN_DURATION;
            }
        }).cache();
        long longFlightsCount = longFlights.count();

        // Filter flights that do not contain any position data
        JavaRDD<Flight> flightsWithPosition = longFlights.filter(new Function<Flight, Boolean>() {
            public Boolean call(Flight flight) throws Exception {
                for (FlightDatum fd : flight.getFlightData()) {
                    if (fd.hasPosition()) {
                        return true;
                    }
                }
                return false;
            }
        }).cache();
        long flightsWithPositionCount = flightsWithPosition.count();

        // Split flights when standing still for a certain time
        JavaRDD<Flight> finalFlights = flightsWithPosition.flatMap(new FlatMapFunction<Flight, Flight>() {
            public Iterable<Flight> call(Flight flight) throws Exception {
                List<Flight> splitFlights = new ArrayList<Flight>();

                // Put flight data in minute wide bins
                SortedMap<Long, List<FlightDatum>> flightDataPerMinute = new TreeMap<Long, List<FlightDatum>>();
                for (FlightDatum fd : flight.getFlightData()) {
                    long minute = (long) (fd.getTime() / 60);
                    List<FlightDatum> flightData = flightDataPerMinute.get(minute);
                    if (flightData == null) {
                        flightData = new ArrayList<FlightDatum>();
                        flightDataPerMinute.put(minute, flightData);
                    }
                    flightData.add(fd);
                }

                // Calculate distance traveled per minute
                List<Double> splitTimes = new ArrayList<Double>();
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
                    List<FlightDatum> lastFlightData = new ArrayList<FlightDatum>();
                    double time = splitTimes.get(0);
                    splitTimes.remove(0);
                    for (FlightDatum fd : flight.getFlightData()) {
                        if (fd.getTime() == time) {
                            splitFlights.add(new Flight(flight.getIcao(), lastFlightData));
                            lastFlightData.clear();
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
        long flightsCount = finalFlights.count();

        // To CSV
        JavaRDD<String> csv = finalFlights.flatMap(new FlatMapFunction<Flight, String>() {
            public Iterable<String> call(Flight flight) throws Exception {
                return flight.toCSV(true);
            }
        });

        // To file
        csv.saveAsTextFile(outputPath);

        // Print statistics
        List<String> statistics = new ArrayList<String>();
        statistics.add(numberOfItemsStatistic("input records", recordsCount));
        statistics.add(numberOfItemsStatistic("input aircraft", aircraftCount));
        statistics.add(numberOfItemsStatistic("flights after splitting on time only            ", initialFlightsCount));
        statistics.add(numberOfItemsStatistic("flights after filtering on time                 ", longFlightsCount));
        statistics.add(numberOfItemsStatistic("flights after filtering on having location data ", flightsWithPositionCount));
        statistics.add(numberOfItemsStatistic("flights after splitting on distance traveled    ", flightsCount));
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

// OLD CODE FOR SPLITTING
//public Iterable<Flight> call(Flight flight) throws Exception {
//    List<Flight> splitFlights = new ArrayList<Flight>();
//
//    // Put flight data in minute wide bins
//    SortedMap<Long, List<FlightDatum>> flightDataPerMinute = new TreeMap<Long, List<FlightDatum>>();
//    for (FlightDatum fd : flight.getFlightData()) {
//        long minute = (long) (fd.getTime() / 60);
//        List<FlightDatum> flightData = flightDataPerMinute.get(minute);
//        if (flightData == null) {
//            flightData = new ArrayList<FlightDatum>();
//            flightDataPerMinute.put(minute, flightData);
//        }
//        flightData.add(fd);
//    }
//
//    // Calculate distance traveled per minute
//    SortedMap<Long, Double> distanceTraveledPerMinute = new TreeMap<Long, Double>();
//    for (long minute : flightDataPerMinute.keySet()) {
//        List<FlightDatum> flightData = flightDataPerMinute.get(minute);
//        Double distanceTraveled = null;
//        Position lastPosition = null;
//        for (FlightDatum fd : flightData) {
//            Position position = fd.getPosition();
//            if (position != null) {
//                if (position.getAltitude() != null && position.getAltitude() > 50) {
//                    distanceTraveled = MIN_DISTANCE;
//                    break;
//                }
//                if (lastPosition != null) {
//                    if (distanceTraveled == null) {
//                        distanceTraveled = 0d;
//                    }
//                    distanceTraveled += lastPosition.distanceTo(position);
//                }
//                lastPosition = position;
//            }
//        }
//        distanceTraveledPerMinute.put(minute, distanceTraveled);
//    }
//
//    // Sliding window (slide per minute) that checks distance traveled during that window
//    List<Double> splitTimes = new ArrayList<Double>();
//    for (long minute : flightDataPerMinute.keySet()) {
//        Double totalDistanceTraveled = distanceTraveledPerMinute.get(minute);
//        if (totalDistanceTraveled == null)
//            continue;
//
//        for (int i = 1; i < 20; i++) {
//            Double distanceTraveled = distanceTraveledPerMinute.get(minute + i);
//            if (distanceTraveled != null) {
//                totalDistanceTraveled += distanceTraveledPerMinute.get(minute + i);
//            }
//        }
//
//        if (totalDistanceTraveled >= MIN_DISTANCE) {
//            List<FlightDatum> flightData = flightDataPerMinute.get(minute);
//            for (int i = 1; i < 20; i++) {
//                flightData = Util.addList(flightData, flightDataPerMinute.get(minute + i));
//            }
//            splitTimes.add(flightData.get(flightData.size() / 2).getTime());
//        }
//    }
//
//    // Split flight if we found moments to split on
//    if (splitTimes.isEmpty()) {
//        splitFlights.add(flight);
//    } else {
//        List<FlightDatum> lastFlightData = new ArrayList<FlightDatum>();
//        double time = splitTimes.get(0);
//        splitTimes.remove(0);
//        for (FlightDatum fd : flight.getFlightData()) {
//            if (fd.getTime() == time) {
//                splitFlights.add(new Flight(flight.getIcao(), lastFlightData));
//                lastFlightData.clear();
//                if (!splitTimes.isEmpty()) {
//                    time = splitTimes.get(0);
//                    splitTimes.remove(0);
//                } else {
//                    time = Double.MAX_VALUE;
//                }
//            }
//            lastFlightData.add(fd);
//        }
//        splitFlights.add(new Flight(flight.getIcao(), lastFlightData));
//    }
//
//    return splitFlights;
//}