package vu.lsde.core.analytics;

import org.opensky.libadsb.Position;
import vu.lsde.core.model.Flight;
import vu.lsde.core.model.FlightDatum;
import vu.lsde.core.util.Grouping;

import java.util.List;
import java.util.SortedMap;

public class FlightAnalyzer {

    public static boolean hadLowSpeedInAir(Flight flight) {
        SortedMap<Long, List<FlightDatum>> flightDataPerMinute = Grouping.groupFlightDataByTimeWindow(flight.getFlightData(), 60);
        for (Iterable<FlightDatum> flightData : flightDataPerMinute.values()) {
            // Check using velocity data first
            Boolean airborne = null;
            Boolean lowSpeed = null;
            for (FlightDatum fd : flightData) {
                if (fd.hasAltitude()) {
                    airborne = fd.getAltitude() > 50;
                    if (!airborne) {
                        break;
                    }
                }
                if (fd.hasVelocity()) {
                    lowSpeed = fd.getVelocity() < 15;
                    if (!lowSpeed) {
                        break;
                    }
                }
            }

            if (airborne == null || !airborne)
                continue;

            if (lowSpeed != null && lowSpeed) {
                return true;
            }

            // Now check using lat/long data
            lowSpeed = null;
            Position lastPosition = null;
            double lastTime = 0;
            for (FlightDatum fd : flightData) {
                if (fd.hasPosition()) {
                    Position position = fd.getPosition();
                    double time = fd.getTime();
                    if (lastPosition != null) {
                        double distance = lastPosition.distanceTo(position);
                        double timeSpan = time - lastTime;
                        // Only calculate distance if there was not too much time between two points, otherwise
                        // it will become too inaccurate
                        if (timeSpan < 10) {
                            lowSpeed = distance / timeSpan < 5;
                        }
                        if (lowSpeed != null && !lowSpeed) {
                            break;
                        }
                    }
                    lastPosition = position;
                    lastTime = time;
                }
            }

            if (lowSpeed != null && lowSpeed) {
                return true;
            }
        }

        return false;
    }

    public static boolean hadHighClimbingAngle(Flight flight) {
        double maxAngle = Math.toRadians(45);
        for (FlightDatum fd : flight.getFlightData()) {
            if (fd.hasVelocity() && fd.hasRateOfClimb()) {
                double velocity = fd.getVelocity();
                double roc = fd.getRateOfClimb();
                if (Math.atan(roc / velocity) > maxAngle) {
                    return true;
                }
            }
        }
        return false;
    }
}
