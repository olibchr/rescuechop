package vu.lsde.jobs;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import scala.Tuple2;
import vu.lsde.core.model.Flight;
import vu.lsde.core.model.FlightDatum;
import vu.lsde.core.model.SensorDatum;

import java.util.ArrayList;
import java.util.List;

import static vu.lsde.jobs.functions.ClassifierFunctions.classifyHelicopterFlights;
import static vu.lsde.jobs.functions.FlightDataFunctions.filterFlightDataMsgs;
import static vu.lsde.jobs.functions.FlightDataFunctions.sensorDataToFlightData;
import static vu.lsde.jobs.functions.FlightFunctions.*;

public class FlightClassificationEvaluator extends JobBase {
    public static void main(String[] args) {
        String inputPath = args[0];
        String outputPath = args[1];

        SparkConf sparkConf = new SparkConf().setAppName("LSDE09 FlightClassificationEvaluator");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        // Get datasets
        JavaRDD<SensorDatum> heliSensorData = readSensorDataCsv(sc, inputPath + "_helis");
        JavaRDD<SensorDatum> planeSensorData = readSensorDataCsv(sc, inputPath + "_planes");

        // Filter out irrelevant messages
        heliSensorData = heliSensorData.filter(filterFlightDataMsgs);
        planeSensorData = planeSensorData.filter(filterFlightDataMsgs);

        // Group by icao
        JavaPairRDD<String, Iterable<SensorDatum>> heliSensorDataByHeli = groupByIcao(heliSensorData);
        JavaPairRDD<String, Iterable<SensorDatum>> planeSensorDataByPlane = groupByIcao(planeSensorData);

        // Map to flight data
        JavaPairRDD<String, Iterable<FlightDatum>> heliFlightDataByHeli = heliSensorDataByHeli.mapToPair(sensorDataToFlightData);
        JavaPairRDD<String, Iterable<FlightDatum>> planeFlightDataByPlane = planeSensorDataByPlane.mapToPair(sensorDataToFlightData);

        heliFlightDataByHeli = heliFlightDataByHeli.filter(new Function<Tuple2<String, Iterable<FlightDatum>>, Boolean>() {
            public Boolean call(Tuple2<String, Iterable<FlightDatum>> t) throws Exception {
                return t._2.iterator().hasNext();
            }
        });
        planeFlightDataByPlane = planeFlightDataByPlane.filter(new Function<Tuple2<String, Iterable<FlightDatum>>, Boolean>() {
            public Boolean call(Tuple2<String, Iterable<FlightDatum>> t) throws Exception {
                return t._2.iterator().hasNext();
            }
        });

        // Map to flights (splitting on time)
        JavaRDD<Flight> heliFlights = flatten(heliFlightDataByHeli.mapToPair(splitFlightDataToFlightsOnTime));
        JavaRDD<Flight> planeFlights = flatten(planeFlightDataByPlane.mapToPair(splitFlightDataToFlightsOnTime));

        // Filter out short flights, split remaining flights on altitude or distance, filter again on short flights
        heliFlights = heliFlights.filter(isLongFlight).flatMap(splitFlightsOnAltitudeOrDistance).filter(isLongFlight);
        planeFlights = planeFlights.filter(isLongFlight).flatMap(splitFlightsOnAltitudeOrDistance).filter(isLongFlight);

        // Group by icao
        JavaPairRDD<String, Iterable<Flight>> heliFlightsByHeli = groupByIcao(heliFlights);
        JavaPairRDD<String, Iterable<Flight>> planeFlightsByPlane = groupByIcao(planeFlights);

        heliFlightsByHeli = heliFlightsByHeli.filter(new Function<Tuple2<String, Iterable<Flight>>, Boolean>() {
            public Boolean call(Tuple2<String, Iterable<Flight>> t) throws Exception {
                return t._2.iterator().hasNext();
            }
        });
        planeFlightDataByPlane.filter(new Function<Tuple2<String, Iterable<FlightDatum>>, Boolean>() {
            public Boolean call(Tuple2<String, Iterable<FlightDatum>> t) throws Exception {
                return t._2.iterator().hasNext();
            }
        });

        // Count amount of aircraft
        long heliCount = heliFlightsByHeli.count();
        long planeCount = planeFlightsByPlane.count();

        // Classify
        JavaPairRDD<String, Iterable<Flight>> truePositives = heliFlightsByHeli.filter(classifyHelicopterFlights);
        JavaPairRDD<String, Iterable<Flight>> falsePositives = planeFlightsByPlane.filter(classifyHelicopterFlights);

        long truePositivesCount = truePositives.count();
        long falsePositivesCount = falsePositives.count();

        // Create stats
        List<String> statistics = new ArrayList<>();
        statistics.add(numberOfItemsStatistic("True positives", truePositivesCount, heliCount));
        statistics.add(numberOfItemsStatistic("False negatives", heliCount - truePositivesCount, heliCount));
        statistics.add(numberOfItemsStatistic("True negatives", planeCount - falsePositivesCount, planeCount));
        statistics.add(numberOfItemsStatistic("False positives", falsePositivesCount, planeCount));
        saveStatisticsAsTextFile(sc, outputPath, statistics);
    }
}
