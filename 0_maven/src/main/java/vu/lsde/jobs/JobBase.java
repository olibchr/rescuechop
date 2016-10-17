package vu.lsde.jobs;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;
import sun.management.Sensor;
import vu.lsde.core.model.Flight;
import vu.lsde.core.model.FlightDatum;
import vu.lsde.core.model.ModelBase;
import vu.lsde.core.model.SensorDatum;

import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;

/**
 * Base class for Spark jobs.
 */
public abstract class JobBase {

    // Methods for loading RDDs

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

                SortedSet<FlightDatum> flightData = new TreeSet<>();
                for (String line : lines) {
                    flightData.add(FlightDatum.fromCSV(line.substring(startFlightDatum)));
                }
                String icao = flightData.first().getIcao();

                return new Flight(icao, flightData);
            }
        });
    }

    // Common RDD transformations

    /**
     * Flatten an RDD containing iterables.
     */
    public static <I extends Iterable<T>, T> JavaRDD<T> flatten(JavaRDD<I> bags) {
        return bags.flatMap(new FlatMapFunction<I, T>() {
            public I call(I ts) throws Exception {
                return ts;
            }
        });
    }

    /**
     * Flatten a PairRDD containing iterable values.
     */
    public static <K, I extends Iterable<T>, T> JavaRDD<T> flatten(JavaPairRDD<K, I> groups) {
        return groups.values().flatMap(new FlatMapFunction<I, T>() {
            public Iterable<T> call(I ts) throws Exception {
                return ts;
            }
        });
    }

    /**
     * Maps models to (icao, model) tuples.
     */
    public static <M extends ModelBase> JavaPairRDD<String, M> toIcaoModelPairs(JavaRDD<M> sensorData) {
        return sensorData.mapToPair(new PairFunction<M, String, M>() {
            public Tuple2<String, M> call(M model) throws Exception {
                return new Tuple2<>(model.getIcao(), model);
            }
        });
    }

    /**
     * Groups models by icao.
     */
    public static <M extends ModelBase> JavaPairRDD<String, Iterable<M>> groupByIcao(JavaRDD<M> sensorData) {
        return sensorData.groupBy(new Function<M, String>() {
            public String call(M m) throws Exception {
                return m.getIcao();
            }
        });
    }

    // Methods for saving RDDs

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

    // Utility methods

    protected static String numberOfItemsStatistic(String itemName, long count) {
        return String.format("Number of %s: %d", itemName, count);
    }

    protected static String numberOfItemsStatistic(String itemName, long count, long parentCount) {
        return String.format("Number of %s: %d (%.2f%%)", itemName, count, 100.0 * count / parentCount);
    }

    protected static void saveStatisticsAsTextFile(JavaSparkContext sc, String outputPath, List<String> statisticsLines) {
        sc.parallelize(statisticsLines, 1).saveAsTextFile(outputPath + "_stats");
    }

}
