package vu.lsde.jobs;

import org.apache.hadoop.mapred.Counters;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import scala.Tuple2;
import vu.lsde.core.model.Flight;
import vu.lsde.core.model.FlightDatum;
import vu.lsde.core.util.Grouping;

import java.util.ArrayList;
import java.util.List;
import java.util.SortedMap;

public class FlightsAnalyzer {
    public static void main(String[] args) {
        Logger log = LogManager.getLogger(Flights.class);
        log.setLevel(Level.INFO);

        String inputPath = args[0];
        String outputPath = args[1];

        SparkConf sparkConf = new SparkConf().setAppName("LSDE09 FlightsAnalyzer");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        // Records
        JavaRDD<String> lines = sc.textFile(inputPath);

        // Map by flight ID
        JavaPairRDD<String, Iterable<String>> linesByFlight = lines.groupBy(new Function<String, String>() {
            public String call(String s) throws Exception {
                int comma = s.indexOf(",");
                return s.substring(0, comma);
            }
        });

        // Convert to flights
        JavaRDD<Flight> flights = linesByFlight.map(new Function<Tuple2<String, Iterable<String>>, Flight>() {
            public Flight call(Tuple2<String, Iterable<String>> tuple) throws Exception {
                String id = tuple._1;
                Iterable<String> lines = tuple._2;
                int startFlightDatum = id.length() + 1;

                List<FlightDatum> flightData = new ArrayList<FlightDatum>();
                for (String line : lines) {
                    flightData.add(FlightDatum.fromCSV(line.substring(startFlightDatum)));
                }
                String icao = flightData.get(0).getIcao();

                return new Flight(icao, flightData);
            }
        });
    }

    public static boolean hadLowSpeedInAir(Flight flight) {
        // put flight data in minute bins
        // for each minute check if it was airborne the whole time, and if it had low velocity at that time
        // if no altitude data, stop
        // if no velocity data, do the same, but using lat+long now


        SortedMap<Long, List<FlightDatum>> flightDataPerMinute = Grouping.groupFlightDataByMinute(flight.getFlightData());
        for (Iterable<FlightDatum> flightData : flightDataPerMinute.values()) {
            boolean hasAltitudeData = false;
            boolean hasVelocityData = false;
            boolean airborne = true;
            boolean lowSpeed = false;
            for (FlightDatum fd : flightData) {
                if (fd.hasAltitude()) {
                    hasAltitudeData = true;
                    airborne = fd.getAltitude() > 0;
                    if (!airborne) {
                        break;
                    }
                }
                if (fd.hasVelocity()) {
                    if (fd.getVelocity() < 15) {
                        lowSpeed = true;
                    }
                }
            }
        }
        return false;
    }

    public static boolean hadHighClimbingAngle(Flight flight) {
        double maxAngle = Math.toRadians(25);
        for (FlightDatum fd : flight.getFlightData()) {
            Double velocity = fd.getVelocity();
            Double roc = fd.getRateOfClimb();
            if (Math.atan(roc/velocity) > maxAngle) {
                return true;
            }
        }
        return false;
    }
}
