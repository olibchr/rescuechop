package vu.lsde.jobs;

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
import vu.lsde.core.model.PlotDatum;
import vu.lsde.core.reducer.SeriesReducer;
import vu.lsde.core.util.Geo;

import java.util.ArrayList;
import java.util.List;

public class FlightsPostProcessor extends JobBase {

    private static class Circle {
        double longitude;
        double latitude;
        double radius;

        Circle(double latitude, double longitude, double radius) {
            this.latitude = latitude;
            this.longitude = longitude;
            this.radius = radius;
        }
    }

    public static void main(String[] args) {
        String inputPath = args[0];
        String outputPath = args[1];

        SparkConf sparkConf = new SparkConf().setAppName("LSDE09 FlightsPostProcessor");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        // Retrieve flights
        JavaRDD<Flight> flights = readFlightsCsv(sc, inputPath);

        // Convert to plot data and reduce it
        JavaRDD<PlotDatum> plotData = flights.flatMap(new FlatMapFunction<Flight, PlotDatum>() {
            public Iterable<PlotDatum> call(Flight flight) throws Exception {
                List<PlotDatum> plotData = PlotDatum.fromFlight(flight);
                return SeriesReducer.reduce(plotData, 1);
            }
        });

        // Create circles that bound flights
        JavaPairRDD<String, Circle> circles = flights.mapToPair(new PairFunction<Flight, String, Circle>() {
            @Override
            public Tuple2<String, Circle> call(Flight flight) throws java.lang.Exception {
                List<Position> positions = new ArrayList<Position>();
                for (FlightDatum fd : flight.getFlightData()) {
                    if (fd.hasPosition()) {
                        positions.add(fd.getPosition());
                    }
                }
                if (positions.isEmpty()) {
                    return null;
                }
                Position center = Geo.findCentralPosition(positions);
                Double maxDist = 0.0;
                Circle maxCircle = null;
                for (Position pos : positions) {
                    if (pos.distanceTo(center) > maxDist) {
                        maxDist = pos.distanceTo(center);
                        maxCircle = new Circle(pos.getLatitude(), pos.getLongitude(), maxDist);
                    }
                }
                return new Tuple2<>(flight.getIcao(), maxCircle);
            }
        });

        // Convert flights to CSV
        JavaRDD<String> flightsCsv = plotData.map(new Function<PlotDatum, String>() {
            public String call(PlotDatum plotDatum) throws Exception {
                return plotDatum.toCsv();
            }
        });

        // Convert circles to CSV
        JavaRDD<String> circlesCsv = circles.flatMap(new FlatMapFunction<Tuple2<String, Circle>, String>() {
            @Override
            public Iterable<String> call(Tuple2<String, Circle> t) throws Exception {
                List<String> lines = new ArrayList<String>();
                if (t._2 != null) {
                    lines.add(t._1 + "," + t._2.latitude + "," + t._2.longitude + "," + t._2.radius);
                }
                return lines;
            }
        });

        // Write to file
        flightsCsv.saveAsTextFile(outputPath + "_flights");
        circlesCsv.saveAsTextFile(outputPath + "_circles");
    }
}