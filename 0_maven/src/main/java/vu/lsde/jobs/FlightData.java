package vu.lsde.jobs;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import vu.lsde.core.model.FlightDatum;
import vu.lsde.core.model.SensorDatum;
import vu.lsde.jobs.functions.FlightDataFunctions;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static vu.lsde.jobs.functions.FlightDataFunctions.sensorDataByAircraftToFlightDataByAircraft;
import static vu.lsde.jobs.functions.SensorDataFunctions.flightDataSensorData;

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
        sensorData = sensorData.filter(flightDataSensorData());
        long filteredRecordsCount = sensorData.count();

        // Group models by icao
        JavaPairRDD<String, Iterable<SensorDatum>> sensorDataByAircraft = groupByIcao(sensorData);

        // Map messages to flight data
        JavaPairRDD<String, Iterable<FlightDatum>> flightDataByAircraft = sensorDataByAircraft.mapToPair(sensorDataByAircraftToFlightDataByAircraft());

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
