package vu.lsde.jobs;

import com.clearspring.analytics.util.Lists;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.Accumulator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;
import vu.lsde.core.model.Flight;
import vu.lsde.core.model.FlightDatum;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class Flights {
    private static final double MIN_TIME_DELTA = 20 * 60;
    private static final double MIN_DURATION = 60;

    public static void main(String[] args) {
        Logger log = LogManager.getLogger(Flights.class);
        log.setLevel(Level.INFO);

        String inputPath = args[0];
        String outputPath = args[1];

        SparkConf sparkConf = new SparkConf().setAppName("LSDE09 FlightData");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        // Load CSV
        JavaRDD<String> records = sc.textFile(inputPath);
        long recordsCount = records.count();

        // Map to model
        final JavaRDD<FlightDatum> flightData = records.map(new Function<String, FlightDatum>() {
            public FlightDatum call(String s) throws Exception {
                return FlightDatum.fromCSV(s);
            }
        });

        // Group by aircraft
        JavaPairRDD<String, Iterable<FlightDatum>> flightDataByAircraft = flightData.groupBy(new Function<FlightDatum, String>() {
            public String call(FlightDatum flightDatum) throws Exception {
                return flightDatum.getIcao();
            }
        });
        long aircraftCount = flightDataByAircraft.count();

        final Accumulator<Integer> rejectedFlightsAcc = sc.accumulator(0);
        final Accumulator<Integer> acceptedFlightsAcc = sc.accumulator(0);

        // Group flight data into flights
        JavaPairRDD<String, Iterable<Flight>> flightsByAircraft = flightDataByAircraft.flatMapValues(new Function<Iterable<FlightDatum>, Iterable<Iterable<Flight>>>() {
            public Iterable<Iterable<Flight>> call(Iterable<FlightDatum> flightData) throws Exception {
                List<FlightDatum> flightDataList = Lists.newArrayList(flightData);

                String icao = flightDataList.get(0).getIcao();
                List<Flight> flights = new ArrayList<Flight>();

                // First do a rough grouping merely on time
                List<FlightDatum> lastFlightData = new ArrayList<FlightDatum>();
                double lastTime = flightDataList.get(0).getTime();
                for (FlightDatum fd : flightDataList) {
                    if (fd.getTime() - lastTime >= MIN_TIME_DELTA) {
                        Flight flight = new Flight(icao, lastFlightData);
                        lastFlightData.clear();
                        if (flight.getDuration() >= MIN_DURATION) {
                            flights.add(flight);
                            acceptedFlightsAcc.add(1);
                        } else {
                            rejectedFlightsAcc.add(1);
                        }
                    }
                    lastFlightData.add(fd);
                    lastTime = fd.getTime();
                }

                return (Iterable) flights;
            }
        });

        // Flatten
        JavaRDD<Flight> flights = flightsByAircraft.flatMap(new FlatMapFunction<Tuple2<String, Iterable<Flight>>, Flight>() {
            public Iterable<Flight> call(Tuple2<String, Iterable<Flight>> tuple) throws Exception {
                return tuple._2;
            }
        });
        long flightsCount = flights.count();

        // To CSV
        JavaRDD<String> csv = flights.map(new Function<Flight, String>() {
            public String call(Flight flight) throws Exception {
                return flight.toCSV();
            }
        });

        // To file
        csv.saveAsTextFile(outputPath);

        // Print statistics
        List<String> statistics = new ArrayList<String>();
        statistics.add(numberOfItemsStatistic("input records", recordsCount));
        statistics.add(numberOfItemsStatistic("input aircraft", aircraftCount));
        statistics.add(numberOfItemsStatistic("rejected short flights", rejectedFlightsAcc.value()));
        statistics.add(numberOfItemsStatistic("resulting flights", flightsCount));
        statistics.add(numberOfItemsStatistic("aircraft with flights", acceptedFlightsAcc.value()));
        saveStatisticsAsTextFile(sc, outputPath, statistics);
    }

    private static String numberOfItemsStatistic(String itemName, long count) {
        return String.format("Number of %s: %d", itemName, count);
    }

    private static String numberOfItemsStatistic(String itemName, long count, long parentCount) {
        return String.format("Number of %s: %d (%.2f%%)", itemName, count, 100.0 * count / parentCount);
    }

    private static void saveStatisticsAsTextFile(JavaSparkContext sc, String outputPath, List<String> statisticsLines) {
        JavaRDD<String> statsRDD = sc.parallelize(statisticsLines).coalesce(1);
        statsRDD.saveAsTextFile(outputPath + "_stats");
    }
}
