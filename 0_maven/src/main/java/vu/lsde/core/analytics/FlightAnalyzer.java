package vu.lsde.core.analytics;

import org.opensky.libadsb.Position;
import vu.lsde.core.model.Flight;
import vu.lsde.core.model.FlightDatum;
import vu.lsde.core.util.Geo;
import vu.lsde.core.util.Grouping;

import java.util.ArrayList;
import java.util.List;
import java.util.SortedMap;

import static vu.lsde.core.util.Util.addIfNotNull;
import static vu.lsde.core.util.Util.angleDistance;
import static vu.lsde.core.util.Util.avgAngle;

public class FlightAnalyzer {

    public static boolean wasHovering(Flight flight) {
        SortedMap<Long, List<FlightDatum>> flightDataPer30Seconds = Grouping.groupFlightDataByTimeWindow(flight.getFlightData(), 30);
        for (Iterable<FlightDatum> flightData : flightDataPer30Seconds.values()) {
            Boolean airborne = null;
            for (FlightDatum fd : flightData) {
                if (fd.hasAltitude()) {
                    airborne = fd.getAltitude() > 30;
                    if (!airborne) {
                        break;
                    }
                }
            }

            if (airborne == null || !airborne) {
                continue;
            }

            List<Position> positions = new ArrayList<>();
            for (FlightDatum fd : flightData) {
                if (fd.hasPosition()) {
                    positions.add(fd.getPosition());
                }
            }

            if (positions.size() < 3) {
                continue;
            }

            boolean hovering = true;
            Position center = Geo.findCentralPosition(positions);
            for (Position position : positions) {
                if (center.distanceTo(position) > 10) {
                    hovering = false;
                    break;
                }
            }

            if (hovering) {
                return true;
            }
        }
        return false;
    }

    public static boolean hadLowSpeedInAir(Flight flight) {
        SortedMap<Long, List<FlightDatum>> flightDataPerMinute = Grouping.groupFlightDataByTimeWindow(flight.getFlightData(), 60);
        for (Iterable<FlightDatum> flightData : flightDataPerMinute.values()) {
            // Check if airborne
            Boolean airborne = null;
            for (FlightDatum fd : flightData) {
                if (fd.hasAltitude()) {
                    airborne = fd.getAltitude() > 30;
                    if (!airborne) {
                        break;
                    }
                }
            }

            if (airborne == null || !airborne) {
                continue;
            }

            // Check speed using velocity data first
            Boolean lowSpeed = null;
            for (FlightDatum fd : flightData) {
                if (fd.hasVelocity()) {
                    lowSpeed = fd.getVelocity() < 15;
                    if (!lowSpeed) {
                        break;
                    }
                }
            }

            if (lowSpeed != null && lowSpeed) {
                return true;
            }
        }

        return false;
    }

    public static boolean hadHighClimbingAngle(Flight flight) {
        double maxAngle = Math.toRadians(50);
        double highClimbingCount = 0;
        for (FlightDatum fd : flight.getFlightData()) {
            if (fd.hasVelocity() && fd.hasRateOfClimb()) {
                double velocity = fd.getVelocity();
                double roc = fd.getRateOfClimb();
                if (Math.atan(Math.abs(roc) / velocity) > maxAngle) {
                    highClimbingCount++;
                } else {
                    highClimbingCount = 0;
                }
                if (highClimbingCount > 0) {
                    return true;
                }
            }
        }
        return false;
    }

    public static boolean crusingAtHelicopterSpeedAndAltitude(Flight flight) {
        SortedMap<Long, List<FlightDatum>> flightDataWindows = Grouping.groupFlightDataByTimeWindow(flight.getFlightData(), 180);
        for (List<FlightDatum> flightData : flightDataWindows.values()) {
            List<Double> headings = new ArrayList<>();
            for (FlightDatum fd : flightData) {
                addIfNotNull(headings, fd.getHeading());
            }
            Double avgHeading = avgAngle(headings);

            if (avgHeading == null) {
                continue;
            }

            boolean cruising = false;
            for (Double heading : headings) {
                cruising = angleDistance(heading, avgHeading) > 5;
                if (!cruising) {
                    break;
                }
            }

            if (!cruising) {
                continue;
            }

            boolean likeHelicopter = false;
            boolean hasAltitude = false;
            boolean hasVelocity = false;
            for (FlightDatum fd : flightData) {
                if (fd.hasAltitude()) {
                    hasAltitude = true;
                    likeHelicopter = fd.getAltitude() > 950 && fd.getAltitude() < 1050;
                    if (!likeHelicopter) {
                        break;
                    }
                }
                if (fd.hasVelocity()) {
                    hasVelocity = true;
                    likeHelicopter = fd.getVelocity() > 85 && fd.getVelocity() < 95;
                    if (!likeHelicopter) {
                        break;
                    }
                }
            }

            if (hasAltitude && hasVelocity && likeHelicopter) {
                return true;
            }
        }
        return false;
    }

    public static boolean landsOrAscendsFromPosition(Flight flight, List<Position> helipads) {
        Position startPosition = flight.getStartPosition();
        Position endPosition = flight.getEndPosition();
        if (startPosition != null && startPosition.getAltitude() != null) {
            if (startPosition.getAltitude() < 100) {
                for (Position helipad : helipads) {
                    if (startPosition.distanceTo(helipad) < 300) {
                        return true;
                    }
                }
            }
            if (endPosition.getAltitude() < 100) {
                for (Position helipad : helipads) {
                    if (endPosition.distanceTo(helipad) < 300) {
                        return true;
                    }
                }
            }
        }
        return false;
    }
}
