package vu.lsde.core.analytics;

import org.opensky.libadsb.Position;
import vu.lsde.core.model.Flight;
import vu.lsde.core.model.FlightDatum;
import vu.lsde.core.util.Geo;
import vu.lsde.core.util.Grouping;

import java.util.ArrayList;
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
                    airborne = fd.getAltitude() > 100;
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

            if (airborne == null)
                continue;

            if (lowSpeed != null) {
                if (airborne && lowSpeed) {
                    return true;
                }
                continue;
            }

            // Now check using lat/long data
//            airborne = null;
//            List<Position> positions = new ArrayList<>();
//            for (FlightDatum fd : flightData) {
//                if (fd.hasAltitude()) {
//                    airborne = fd.getAltitude() > 100;
//                    if (!airborne) {
//                        break;
//                    }
//                }
//                if (fd.hasPosition()) {
//                    positions.add(fd.getPosition());
//                }
//            }
//            Position center = Geo.findCentralPosition(positions);
//            for (Position position : positions) {
//                if (position.distanceTo(center) < 50) {
//                    return true;
//                }
//            }
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
