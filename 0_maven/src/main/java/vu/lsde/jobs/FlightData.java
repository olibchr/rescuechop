package vu.lsde.jobs;

import com.clearspring.analytics.util.Lists;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.opensky.libadsb.Position;
import org.opensky.libadsb.PositionDecoder;
import org.opensky.libadsb.msgs.*;
import scala.Tuple2;
import vu.lsde.core.model.FlightDatum;
import vu.lsde.core.model.SensorDatum;
import vu.lsde.jobs.functions.FlightDataFunctions;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Maps sensor data to aircraft positions.
 */
public class FlightData extends JobBase {

    public static void main(String[] args) throws IOException {
        String inputPath = args[0];
        String outputPath = args[1];

        SparkConf sparkConf = new SparkConf().setAppName("LSDE09 FlightData");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        // Load sensor data
        JavaRDD<SensorDatum> sensorData = readSensorDataCsv(sc, inputPath);
        long recordsCount = sensorData.count();

        // Filter position and velocity messages
        sensorData = sensorData.filter(FlightDataFunctions.filterFlightDataMsgs);
        long filteredRecordsCount = sensorData.count();

        // Group models by icao
        JavaPairRDD<String, Iterable<SensorDatum>> sensorDataByAircraft = groupByIcao(sensorData);

        // Map messages to flight data
        JavaPairRDD<String, Iterable<FlightDatum>> flightDataByAircraft = sensorDataByAircraft.mapToPair(FlightDataFunctions.sensorDataToFlightData);

        // Flatten
        JavaRDD<FlightDatum> flightData = flatten(flightDataByAircraft).cache();

        // Write to CSV
        saveAsCsv(flightData, outputPath);

        // Get statistics on flight data
        long flightDataCount = flightData.count();
        long positionDataCount = flightData.filter(new Function<FlightDatum, Boolean>() {
            public Boolean call(FlightDatum flightDatum) throws Exception {
                return flightDatum.getLatitude() != null;
            }
        }).count();
        long altitudeDataCount = flightData.filter(new Function<FlightDatum, Boolean>() {
            public Boolean call(FlightDatum flightDatum) throws Exception {
                return flightDatum.getAltitude() != null;
            }
        }).count();
        long velocityDataCount = flightData.filter(new Function<FlightDatum, Boolean>() {
            public Boolean call(FlightDatum flightDatum) throws Exception {
                return flightDatum.getVelocity() != null;
            }
        }).count();
        long rocDataCount = flightData.filter(new Function<FlightDatum, Boolean>() {
            public Boolean call(FlightDatum flightDatum) throws Exception {
                return flightDatum.getRateOfClimb() != null;
            }
        }).count();

        // Print statistics
        List<String> statistics = new ArrayList<>();
        statistics.add(numberOfItemsStatistic("input records     ", recordsCount));
        statistics.add(numberOfItemsStatistic("filtered records  ", filteredRecordsCount));
        statistics.add(numberOfItemsStatistic("output flight data", flightDataCount));
        statistics.add(numberOfItemsStatistic("position data     ", positionDataCount, flightDataCount));
        statistics.add(numberOfItemsStatistic("altitude data     ", altitudeDataCount, flightDataCount));
        statistics.add(numberOfItemsStatistic("velocity data     ", velocityDataCount, flightDataCount));
        statistics.add(numberOfItemsStatistic("rate of climb data", rocDataCount, flightDataCount));
        saveStatisticsAsTextFile(sc, outputPath, statistics);
    }


}
