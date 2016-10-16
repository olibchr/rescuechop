package vu.lsde.jobs;

import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import vu.lsde.core.model.Flight;
import vu.lsde.core.model.PlotDatum;
import vu.lsde.core.reducer.SeriesReducer;

import java.util.List;

public class FlightsPostProcessor extends JobBase {
    public static void main(String[] args) {
        Logger log = LogManager.getLogger(Flights.class);
        log.setLevel(Level.INFO);

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

        // Convert to CSV
        JavaRDD<String> csv = plotData.map(new Function<PlotDatum, String>() {
            public String call(PlotDatum plotDatum) throws Exception {
                return plotDatum.toCsv();
            }
        });

        // Write to file
        csv.saveAsTextFile(outputPath);
    }
}
