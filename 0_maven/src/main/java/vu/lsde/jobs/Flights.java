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

        final Accumulator<Integer> mergedFlightDataAcc = sc.accumulator(0);

        // Merge flight data that are from the same timestamp
        JavaPairRDD<String, List<FlightDatum>> mergedFlightDataByAircraft = flightDataByAircraft.mapToPair(new PairFunction<Tuple2<String, Iterable<FlightDatum>>, String, List<FlightDatum>>() {
            public Tuple2<String, List<FlightDatum>> call(Tuple2<String, Iterable<FlightDatum>> tuple) throws Exception {
                String icao = tuple._1;
                Iterable<FlightDatum> flightData = tuple._2;

                // Map to list and sort
                List<FlightDatum> flightDataList = Lists.newArrayList(flightData);
                Collections.sort(flightDataList);

                List<FlightDatum> mergedFlightData = new ArrayList<FlightDatum>();
                List<FlightDatum> lastFlightData = new ArrayList<FlightDatum>();
                double lastTime = flightDataList.get(0).getTime();
                for (FlightDatum fd : flightDataList) {
                    if (fd.getTime() != lastTime) {
                        FlightDatum merged = FlightDatum.mergeFlightData(icao, lastTime, lastFlightData);
                        mergedFlightData.add(merged);

                        lastFlightData.clear();
                        lastTime = fd.getTime();
                    }
                    lastFlightData.add(fd);
                }
                FlightDatum merged = FlightDatum.mergeFlightData(icao, lastTime, lastFlightData);
                mergedFlightData.add(merged);

                mergedFlightDataAcc.add(mergedFlightData.size());

                return new Tuple2<String, List<FlightDatum>>(icao, mergedFlightData);
            }
        });

        final Accumulator<Integer> shortFlightsAcc = sc.accumulator(0);

        // Group flight data into flights
        JavaPairRDD<String, Iterable<Flight>> flightsByAircraft = mergedFlightDataByAircraft.flatMapValues(new Function<List<FlightDatum>, Iterable<Iterable<Flight>>>() {
            public Iterable<Iterable<Flight>> call(List<FlightDatum> flightData) throws Exception {
                String icao = flightData.get(0).getIcao();
                List<Flight> flights = new ArrayList<Flight>();

                // First do a rough grouping merely on time
                List<FlightDatum> lastFlightData = new ArrayList<FlightDatum>();
                double lastTime = flightData.get(0).getTime();
                for (FlightDatum fd : flightData) {
                    if (fd.getTime() - lastTime >= MIN_TIME_DELTA) {
                        Flight flight = new Flight(icao, lastFlightData);
                        lastFlightData.clear();
                        if (flight.getDuration() >= MIN_DURATION) {
                            flights.add(flight);
                        } else {
                            shortFlightsAcc.add(1);
                        }
                    }
                    lastFlightData.add(fd);
                    lastTime = fd.getTime();
                }

                return (Iterable) flights;
            }
        });

        // Count aircraft with flights
        long aircraftWithFlightsCount = flightsByAircraft.filter(new Function<Tuple2<String, Iterable<Flight>>, Boolean>() {
            public Boolean call(Tuple2<String, Iterable<Flight>> tuple) throws Exception {
                return !tuple._2.iterator().hasNext();
            }
        }).count();

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
        statistics.add(numberOfItemsStatistic("records after merging", mergedFlightDataAcc.value()));
        statistics.add(numberOfItemsStatistic("rejected short flights", shortFlightsAcc.value()));
        statistics.add(numberOfItemsStatistic("resulting flights", flightsCount));
        statistics.add(numberOfItemsStatistic("aircraft with flights", aircraftWithFlightsCount));
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
