package vu.lsde.jobs.functions;


import org.apache.spark.api.java.function.Function;
import org.apache.spark.broadcast.Broadcast;
import org.opensky.libadsb.Position;
import scala.Tuple2;
import vu.lsde.core.analytics.FlightAnalyzer;
import vu.lsde.core.model.Flight;

import java.util.List;

/**
 * Functions that are used for identifying helicopters and rescue helicopters.
 */
public class ClassifierFunctions {

    public static Function<Tuple2<String, Iterable<Flight>>, Boolean> classifyHelicoperFlights(final Broadcast<List<Position>> helipads) {
        return new Function<Tuple2<String, Iterable<Flight>>, Boolean>() {
            public Boolean call(Tuple2<String, Iterable<Flight>> t) throws Exception {
                double score = 0;
                for (Flight flight : t._2) {
                    if (FlightAnalyzer.wasHovering(flight)) {
                        score++;
                        break;
                    }
                }
                for (Flight flight : t._2) {
                    if (FlightAnalyzer.hadLowSpeedInAir(flight)) {
                        score++;
                        break;
                    }
                }
                if (score > 1) {
                    return true;
                }
                for (Flight flight : t._2) {
                    if (FlightAnalyzer.hadHighClimbingAngle(flight)) {
                        score++;
                        break;
                    }
                }
                if (score > 1) {
                    return true;
                }
                for (Flight flight : t._2) {
                    if (FlightAnalyzer.crusingAtHelicopterSpeedAndAltitude(flight)) {
                        score++;
                        break;
                    }
                }
                if (score > 1) {
                    return true;
                }
                for (Flight flight : t._2) {
                    if (FlightAnalyzer.landsOrAscendsFromPosition(flight, helipads.getValue())) {
                        score++;
                        break;
                    }
                }
                return score > 1;
            }
        };
    }

    public static Function<Tuple2<String, Iterable<Flight>>, Boolean> classifyResuceChopperFlights(final Broadcast<List<Position>> hospitals) {
        return new Function<Tuple2<String, Iterable<Flight>>, Boolean>() {
            @Override
            public Boolean call(Tuple2<String, Iterable<Flight>> t) throws Exception {
                for (Flight flight : t._2) {
                    if (FlightAnalyzer.landsOrAscendsFromPosition(flight, hospitals.getValue())) {
                        return true;
                    }
                }
                return false;
            }
        };
    }
}
