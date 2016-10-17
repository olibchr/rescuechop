package vu.lsde.jobs.functions;


import org.apache.spark.api.java.function.Function;
import scala.Tuple2;
import vu.lsde.core.analytics.FlightAnalyzer;
import vu.lsde.core.model.Flight;

public class ClassifierFunctions {
    public static Function<Tuple2<String, Iterable<Flight>>, Boolean> classifyHelicopterFlights = new Function<Tuple2<String, Iterable<Flight>>, Boolean>() {
        public Boolean call(Tuple2<String, Iterable<Flight>> t) throws Exception {
            for (Flight flight : t._2) {
                if (FlightAnalyzer.hadLowSpeedInAir(flight)) {
                    return true;
                }
                if (FlightAnalyzer.hadHighClimbingAngle(flight)) {
                    return true;
                }
            }
            return false;
        }
    };
}
