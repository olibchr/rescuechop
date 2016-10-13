package vu.lsde.jobs;

import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import scala.Tuple2;
import vu.lsde.core.model.Flight;
import vu.lsde.core.model.FlightDatum;
import vu.lsde.core.model.PlotDatum;
import vu.lsde.core.reducer.SeriesReducer;
import vu.lsde.core.util.Grouping;

import java.util.ArrayList;
import java.util.List;
import java.util.SortedMap;

public class FlightsPostProcessor {
    public static void main(String[] args) {
        Logger log = LogManager.getLogger(Flights.class);
        log.setLevel(Level.INFO);

        String inputPath = args[0];
        String outputPath = args[1];

        SparkConf sparkConf = new SparkConf().setAppName("LSDE09 FlightsPostProcessor");
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

        // Convert to plot data and reduce it
        JavaRDD<PlotDatum> plotData = flights.flatMap(new FlatMapFunction<Flight, PlotDatum>() {
            public Iterable<PlotDatum> call(Flight flight) throws Exception {
                List<PlotDatum> plotData = PlotDatum.fromFlight(flight);
                return SeriesReducer.reduce(plotData, 5);
            }
        });

        // Convert to CSV
        JavaRDD<String> csv = plotData.map(new Function<PlotDatum, String>() {
            public String call(PlotDatum plotDatum) throws Exception {
                return plotDatum.toCSV();
            }
        });

        // Write to file
        csv.saveAsTextFile(outputPath);
    }
}
