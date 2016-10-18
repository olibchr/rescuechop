package vu.lsde.jobs.functions;

import com.clearspring.analytics.util.Lists;
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

public class FlightFunctions {
    // Maximum of time between two flight data points in the same flight in seconds
    private static final double MAX_TIME_DELTA = 20 * 60;
    // Minimum duration of a flight in seconds
    private static final double MIN_DURATION = 2 * 60;
    // Minimum distance an aircraft should move in a minute in meters
    private static final double MIN_DISTANCE = 8;
    // Minimum altitude for an aircraft to consider it flying
    private static final double MIN_ALTITUDE = 30;

    // FUNCTIONS

    public static PairFunction<Tuple2<String, Iterable<FlightDatum>>, String, Iterable<Flight>> splitFlightDataOnTime() {
        return new PairFunction<Tuple2<String, Iterable<FlightDatum>>, String, Iterable<Flight>>() {
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
        };
    }

    public static FlatMapFunction<Flight, Flight> splitflightsOnLandingAndLiftoff() {
        return new FlatMapFunction<Flight, Flight>() {
            public Iterable<Flight> call(Flight flight) throws Exception {
                Iterable<Flight> flights = splitFlightOnAltitude(flight);

                if (flights == null) {
                    flights = splitFlightOnDistance(flight);
                }

                return flights;
            }
        };
    }

    public static Function<Flight, Boolean> noShortFlight() {
        return new Function<Flight, Boolean>() {
            public Boolean call(Flight flight) throws Exception {
                return flight.getDuration() > MIN_DURATION;
            }
        };
    }

    public static Function<Flight, Boolean> hasPositionData() {
        return new Function<Flight, Boolean>() {
            public Boolean call(Flight flight) throws Exception {
                for (FlightDatum fd : flight.getFlightData()) {
                    if (fd.hasPosition()) {
                        return true;
                    }
                }
                return false;
            }
        };
    }

    // PRIVATE METHODS

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
                    if (position.getAltitude() != null && position.getAltitude() > MIN_ALTITUDE)
                        continue;

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
