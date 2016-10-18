package vu.lsde.jobs;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.opensky.libadsb.Position;
import vu.lsde.core.model.Flight;
import vu.lsde.core.model.SensorDatum;
import vu.lsde.core.services.HelipadPositionsService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static vu.lsde.jobs.JobBase.flatten;
import static vu.lsde.jobs.functions.ClassifierFunctions.classifyHelicoperFlights;
import static vu.lsde.jobs.functions.FlightDataFunctions.mergeFlightData;
import static vu.lsde.jobs.functions.FlightDataFunctions.onlyFlightDataMsgs;
import static vu.lsde.jobs.functions.FlightDataFunctions.sensorDataByAircraftToFlightDataByAircraft;
import static vu.lsde.jobs.functions.FlightFunctions.noShortFlight;
import static vu.lsde.jobs.functions.FlightFunctions.splitFlightDataOnTime;
import static vu.lsde.jobs.functions.FlightFunctions.splitflightsOnLandingAndLiftoff;
import static vu.lsde.jobs.functions.SensorDataFunctions.rotorcraftSensorData;

/**
 * Maps sensor data to aircraft positions.
 */
public class HelicopterIdentifier extends JobBase {

    public static void main(String[] args) throws IOException {
        String inputPath = args[0];
        String outputPath = args[1];

        SparkConf sparkConf = new SparkConf().setAppName("LSDE09 HelicopterIdentifier");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        // Get helipad data
        Broadcast<List<Position>> helipads = sc.broadcast(HelipadPositionsService.getHelipadPositions(sc));

        // Load sensor data
        JavaRDD<SensorDatum> sensorData = readSensorDataCsv(sc, inputPath);
        long recordsCount = sensorData.count();

        // Find rough rotorcraft flights by description
        JavaPairRDD<String, Iterable<Flight>> rotorcraftFlightsByRotorcraft = sensorData
                .groupBy(JobBase.<SensorDatum>modelGetIcao())
                .filter(rotorcraftSensorData())
                .mapToPair(sensorDataByAircraftToFlightDataByAircraft())
                .mapToPair(mergeFlightData())
                .mapToPair(splitFlightDataOnTime());
        long rotorcraftCount = rotorcraftFlightsByRotorcraft.count();

        // Get final rotorcraft flights
        JavaRDD<Flight> rotorcraftFlights = flatten(rotorcraftFlightsByRotorcraft)
                .filter(noShortFlight())
                .flatMap(splitflightsOnLandingAndLiftoff())
                .filter(noShortFlight());

        // Find rough flights for unknown aircraft
        JavaPairRDD<String, Iterable<Flight>> otherFlightsByAircraft = sensorData
                .filter(onlyFlightDataMsgs())
                .groupBy(JobBase.<SensorDatum>modelGetIcao())
                .subtractByKey(rotorcraftFlightsByRotorcraft)
                .mapToPair(sensorDataByAircraftToFlightDataByAircraft())
                .mapToPair(mergeFlightData())
                .mapToPair(splitFlightDataOnTime());
        long otherAircraftCount = otherFlightsByAircraft.count();

        // Get final flights for unknown aircraft
        JavaRDD<Flight> otherFlights = flatten(otherFlightsByAircraft)
                .filter(noShortFlight())
                .flatMap(splitflightsOnLandingAndLiftoff())
                .filter(noShortFlight());

        // Classify helicopters in unknown aircraft
        JavaPairRDD<String, Iterable<Flight>> likelyHelicopterFlightsByAircraft = otherFlights
                .groupBy(JobBase.<Flight>modelGetIcao())
                .filter(classifyHelicoperFlights(helipads));
        long likelyHelicoptersCount = likelyHelicopterFlightsByAircraft.count();

        JavaRDD<Flight> likelyHelicopterFlights = flatten(likelyHelicopterFlightsByAircraft);

        // Merge certain and likely helicopter flights
        JavaRDD<Flight> helicopterFlights = rotorcraftFlights.union(likelyHelicopterFlights);
        long helicopterFlightsCount = helicopterFlights.count();

        // Print statistics
        List<String> statistics = new ArrayList<>();
        saveStatisticsAsTextFile(sc, outputPath, statistics);
    }


}
