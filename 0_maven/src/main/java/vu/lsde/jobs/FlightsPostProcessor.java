package vu.lsde.jobs;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.opensky.libadsb.Position;
import scala.Tuple2;
import vu.lsde.core.model.Flight;
import vu.lsde.core.model.PlotDatum;
import vu.lsde.core.reducer.SeriesReducer;

import java.util.ArrayList;
import java.util.List;

/**
 * Post processes generated flights (in CSV format) for visualization purposes.
 */
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

        String toCsv() {
            return this.latitude + "," + this.longitude + "," + this.radius;
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
                if (!plotData.isEmpty()) {
                    plotData = SeriesReducer.reduce(plotData, 1);
                }
                return plotData;
            }
        });

        // Group plot data by flight
        JavaPairRDD<String, Iterable<PlotDatum>> plotDataByFlight = plotData.groupBy(new Function<PlotDatum, String>() {
            @Override
            public String call(PlotDatum plotDatum) throws Exception {
                return plotDatum.getFlightID();
            }

        });

        // Create circles representing possible coverage of flight
        JavaPairRDD<String, Circle> circleByFlight = plotDataByFlight.mapValues(new Function<Iterable<PlotDatum>, Circle>() {
            @Override
            public Circle call(Iterable<PlotDatum> plotData) throws Exception {
                List<Position> positions = new ArrayList<>();
                double firstTime = Double.MAX_VALUE;
                Position center = null;
                for (PlotDatum pd : plotData) {
                    if (firstTime > pd.getTime()) {
                        center = pd.getPosition();
                        firstTime = pd.getTime();
                    }
                    positions.add(pd.getPosition());
                }
                if (center == null) {
                    return null;
                }

                double radius = 0;
                for (Position position : positions) {
                    radius = Math.max(radius, center.distanceTo(position));
                }
                return new Circle(center.getLatitude(), center.getLongitude(), radius);
            }
        });

        // Sort plot data
        plotData = plotData.sortBy(new Function<PlotDatum, PlotDatum>() {
            @Override
            public PlotDatum call(PlotDatum plotDatum) throws Exception {
                return plotDatum;
            }
        }, true, 1);
        
        // Convert flights to CSV
        JavaRDD<String> flightsCsv = plotData.map(new Function<PlotDatum, String>() {
            public String call(PlotDatum plotDatum) throws Exception {
                return plotDatum.toCsv();
            }
        });

        // Convert circles to CSV
        JavaRDD<String> circlesCsv = circleByFlight.flatMap(new FlatMapFunction<Tuple2<String, Circle>, String>() {
            @Override
            public Iterable<String> call(Tuple2<String, Circle> t) throws Exception {
                List<String> lines = new ArrayList<String>();
                if (t._2 != null) {
                    lines.add(t._1 + "," + t._2.toCsv());
                }
                return lines;
            }
        });

        // Write to file
        flightsCsv.saveAsTextFile(outputPath + "_flights");
        circlesCsv.saveAsTextFile(outputPath + "_circles");
    }
}