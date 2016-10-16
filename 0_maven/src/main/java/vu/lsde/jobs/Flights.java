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

import java.util.*;

public class Flights extends JobBase {
    // Maximum of time between two flight data points in the same flight in seconds
    private static final double MAX_TIME_DELTA = 20 * 60;
    // Minimum duration of a flight in seconds
    private static final double MIN_DURATION = 60;
    // Minimum distance an aircraft should move in a minute in meters
    private static final double MIN_DISTANCE = 8;
    // Minimum altitude for an aircraft to consider it flying
    private static final double MIN_ALTITUDE = 10;

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
        JavaRDD<Flight> flights = flatten(flightsByAircraft).cache();
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

        // Split flights on altitude, or position if that's not possible
        flights = flights.flatMap(new FlatMapFunction<Flight, Flight>() {
            public Iterable<Flight> call(Flight flight) throws Exception {
                Iterable<Flight> flights = splitFlightOnAltitude(flight);

                if (flights == null) {
                    flights = splitFlightOnDistance(flight);
                }

                return flights;
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

        long outputAircraftCount = flights.groupBy(new Function<Flight, String>() {
            public String call(Flight flight) throws Exception {
                return flight.getIcao();
            }
        }).count();

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

    private static Iterable<Flight> splitFlightOnAltitude(Flight flight) {
        // First check if there is enough altitude data to split on altitude
        int altitudeCount = 0;
        for (FlightDatum fd : flight.getFlightData()) {
            if (fd.hasAltitude()) {
                altitudeCount++;
            }
        }
        if ((double) altitudeCount / flight.getFlightData().size() < 0.3) {
            return null;
        }

        // If enough data, start splitting
        List<Flight> newFlights = new ArrayList<>();
        SortedSet<FlightDatum> lastFlightData = new TreeSet<>();

        // Whether the aircraft was in the air the last time we checked
        Boolean wasAirborne = null;
        // Whether we should create a new flight soon
        boolean newFlight = false;
        for (FlightDatum fd : flight.getFlightData()) {
            if (fd.hasAltitude()) {
                boolean isAirborne = fd.getAltitude() >= MIN_ALTITUDE;
                if (wasAirborne != null) {
                    if (isAirborne != wasAirborne) {
                        if (!isAirborne) {
                            newFlight = true;
                            // Don't create a new flight just yet, wait until we find the first position data
                        }
                    }
                }
                wasAirborne = isAirborne;
            }
            if (wasAirborne == null || wasAirborne || newFlight) {
                lastFlightData.add(fd);
            }
            if (newFlight && fd.hasPosition() || newFlight && wasAirborne) {
                newFlights.add(new Flight(flight.getIcao(), lastFlightData));
                lastFlightData = new TreeSet<>();
                newFlight = false;
            }
        }
        if (wasAirborne) {
            newFlights.add(new Flight(flight.getIcao(), lastFlightData));
        }

        return newFlights;
    }

    private static Iterable<Flight> splitFlightOnDistance(Flight flight) {
        List<Flight> newFlights = new ArrayList<>();
        SortedSet<FlightDatum> lastFlightData = new TreeSet<>();

        // Put flight data in minute wide bins
        SortedMap<Long, List<FlightDatum>> flightDataPerMinute = Grouping.groupFlightDataByTimeWindow(flight.getFlightData(), 60);

        // Calculate distance traveled per minute
        for (long minute : flightDataPerMinute.keySet()) {
            List<FlightDatum> flightData = flightDataPerMinute.get(minute);
            List<Position> positions = new ArrayList<>();

            double startTime = -1;
            double endTime = -1;
            for (FlightDatum fd : flightData) {
                Position position = fd.getPosition();
                if (position != null) {
                    positions.add(position);
                    if (startTime < 0) {
                        startTime = fd.getTime();
                    }
                    endTime = fd.getTime();
                }
            }

            if (positions.isEmpty() || endTime - startTime < 30)
                continue;

            boolean split = true;
            Position center = Geo.findCentralPosition(positions);
            for (Position position : positions) {
                if (center.distanceTo(position) > MIN_DISTANCE) {
                    split = false;
                }
            }

            if (split) {
                newFlights.add(new Flight(flight.getIcao(), lastFlightData));
                lastFlightData = new TreeSet<>();
            } else {
                lastFlightData.addAll(flightData);
            }
        }

        if (!lastFlightData.isEmpty()) {
            newFlights.add(new Flight(flight.getIcao(), lastFlightData));
        }

        return newFlights;
    }
}