package vu.lsde.jobs;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import scala.Tuple2;
import vu.lsde.core.model.Flight;
import vu.lsde.core.model.FlightDatum;
import vu.lsde.core.model.ModelBase;
import vu.lsde.core.model.SensorDatum;

import java.util.ArrayList;
import java.util.List;

/**
 * Utility class that contains functions for RDDs that are often used in various jobs.
 */
public class Transformations {

    /**
     * Loads SensorDatum objects from a CSV encoded file into an RDD
     *
     * @param sc
     * @param inputPath
     * @return
     */
    public static JavaRDD<SensorDatum> readSensorDataCsv(JavaSparkContext sc, String inputPath) {
        return sc.textFile(inputPath).map(new Function<String, SensorDatum>() {
            public SensorDatum call(String s) throws Exception {
                return SensorDatum.fromCSV(s);
            }
        });
    }

    /**
     * Loads FlightDatum objects from a CSV encoded file into an RDD
     *
     * @param sc
     * @param inputPath
     * @return
     */
    public static JavaRDD<FlightDatum> readFlightDataCsv(JavaSparkContext sc, String inputPath) {
        return sc.textFile(inputPath).map(new Function<String, FlightDatum>() {
            public FlightDatum call(String s) throws Exception {
                return FlightDatum.fromCSV(s);
            }
        });
    }

    /**
     * Loads Flight objects from a CSV encoded file into an RDD
     * @param sc
     * @param inputPath
     * @return
     */
    public static JavaRDD<Flight> readFlightsCsv(JavaSparkContext sc, String inputPath) {
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
        return linesByFlight.map(new Function<Tuple2<String, Iterable<String>>, Flight>() {
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

    /**
     * Flatten an RDD containing iterables.
     *
     * @param bags
     * @param <T>
     * @return
     */
    public static <T> JavaRDD<T> flatten(JavaRDD<Iterable<T>> bags) {
        return bags.flatMap(new FlatMapFunction<Iterable<T>, T>() {
            public Iterable<T> call(Iterable<T> ts) throws Exception {
                return ts;
            }
        });
    }

    /**
     * Flatten a PairRDD containing iterable values.
     *
     * @param groups
     * @param <S>
     * @param <T>
     * @return
     */
    public static <S, T> JavaRDD<T> flatten(JavaPairRDD<S, Iterable<T>> groups) {
        return groups.values().flatMap(new FlatMapFunction<Iterable<T>, T>() {
            public Iterable<T> call(Iterable<T> ts) throws Exception {
                return ts;
            }
        });
    }

    /**
     * Save an RDD containing model objects as a CSV file.
     *
     * @param models
     * @param outputPath
     * @param <T>
     */
    public static <T extends ModelBase> void saveAsCsv(JavaRDD<T> models, String outputPath) {
        models.map(new Function<T, String>() {
            public String call(T modelBase) throws Exception {
                return modelBase.toCsv();
            }
        }).saveAsTextFile(outputPath);
    }
}
