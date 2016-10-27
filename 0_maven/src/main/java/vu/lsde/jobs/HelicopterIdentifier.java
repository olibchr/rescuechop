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
import vu.lsde.core.services.HospitalPositionsService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static vu.lsde.jobs.functions.ClassifierFunctions.classifyHelicoperFlights;
import static vu.lsde.jobs.functions.ClassifierFunctions.classifyResuceChopperFlights;
import static vu.lsde.jobs.functions.FlightDataFunctions.hasFlightData;
import static vu.lsde.jobs.functions.FlightDataFunctions.mergeFlightData;
import static vu.lsde.jobs.functions.FlightDataFunctions.sensorDataByAircraftToFlightDataByAircraft;
import static vu.lsde.jobs.functions.FlightFunctions.*;
import static vu.lsde.jobs.functions.SensorDataFunctions.flightDataSensorData;
import static vu.lsde.jobs.functions.SensorDataFunctions.rotorcraftSensorDataByAircraft;

/**
 * Takes sensor data in CSV format as input, identifies (rescue) helicopters and outputs their flights.
 */
public class HelicopterIdentifier extends JobBase {

    public static void main(String[] args) throws IOException {
        String inputPath = args[0];
        String outputPath = args[1];

        SparkConf sparkConf = new SparkConf().setAppName("LSDE09 HelicopterIdentifier");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        // Get helipad data
        Broadcast<List<Position>> helipads = sc.broadcast(HelipadPositionsService.getHelipadPositions(sc));
        Broadcast<List<Position>> hospitals = sc.broadcast(HospitalPositionsService.getHospitalPositions(sc));

        // Load sensor data
        JavaRDD<SensorDatum> sensorData = readSensorDataCsv(sc, inputPath);

        // Find rough rotorcraft flights by description
        JavaPairRDD<String, Iterable<Flight>> rotorcraftFlightsByRotorcraft = sensorData
                .groupBy(JobBase.<SensorDatum>modelGetIcao())
                .filter(rotorcraftSensorDataByAircraft())
                .mapToPair(sensorDataByAircraftToFlightDataByAircraft())
                .filter(hasFlightData())
                .mapToPair(mergeFlightData())
                .mapToPair(splitFlightDataOnTime());
        long rotorcraftCount = rotorcraftFlightsByRotorcraft.count();

        // Get final rotorcraft flights
        JavaRDD<Flight> rotorcraftFlights = flatten(rotorcraftFlightsByRotorcraft)
                .filter(noShortFlight())
                .filter(hasPositionData())
                .flatMap(splitflightsOnLandingAndLiftoff())
                .filter(noShortFlight());
        long rotorcraftFlightsCount = rotorcraftFlights.count();
        long rotorcraftWithFlightsCount = rotorcraftFlights.groupBy(JobBase.<Flight>modelGetIcao()).count();

        // Find rough flights for unknown aircraft
        JavaPairRDD<String, Iterable<Flight>> otherFlightsByAircraft = sensorData
                .filter(flightDataSensorData())
                .groupBy(JobBase.<SensorDatum>modelGetIcao())
                .subtractByKey(rotorcraftFlightsByRotorcraft)
                .mapToPair(sensorDataByAircraftToFlightDataByAircraft())
                .filter(hasFlightData())
                .mapToPair(mergeFlightData())
                .mapToPair(splitFlightDataOnTime());
        long otherAircraftCount = otherFlightsByAircraft.count();

        // Get final flights for unknown aircraft
        JavaRDD<Flight> otherFlights = flatten(otherFlightsByAircraft)
                .filter(noShortFlight())
                .filter(hasPositionData())
                .flatMap(splitflightsOnLandingAndLiftoff())
                .filter(noShortFlight());
        long otherFlightsCount = otherFlights.count();
        long otherAircraftWithFlightsCount = otherFlights.groupBy(JobBase.<Flight>modelGetIcao()).count();

        // Filter flights without altitude data, classifier can't use those
        otherFlights = otherFlights.filter(hasAltitudeData());
        long otherFlightsWithAltitudeCount = otherFlights.count();
        long otherAircraftWithAltitudeCount = otherFlights.groupBy(JobBase.<Flight>modelGetIcao()).count();

        // Classify helicopters in unknown aircraft
        JavaPairRDD<String, Iterable<Flight>> likelyHelicopterFlightsByAircraft = otherFlights
                .groupBy(JobBase.<Flight>modelGetIcao())
                .filter(classifyHelicoperFlights(helipads));
        long likelyHelicoptersCount = likelyHelicopterFlightsByAircraft.count();

        JavaRDD<Flight> likelyHelicopterFlights = flatten(likelyHelicopterFlightsByAircraft);
        long likelyHelicopterFlightsCount = likelyHelicopterFlights.count();

        // Merge certain and likely helicopter flights
        JavaRDD<Flight> helicopterFlights = rotorcraftFlights.union(likelyHelicopterFlights);
        long helicopterFlightsCount = helicopterFlights.count();
        long helicopterCount = helicopterFlights.groupBy(JobBase.<Flight>modelGetIcao()).count();

        // Find rescue choppers
        JavaPairRDD<String, Iterable<Flight>> flightsByRescueChoppers = helicopterFlights
                .groupBy(JobBase.<Flight>modelGetIcao())
                .filter(classifyResuceChopperFlights(hospitals));
        long rescueChopperCount = flightsByRescueChoppers.count();

        JavaRDD<Flight> rescueChopperFlights = flatten(flightsByRescueChoppers);
        long rescueChopperFlightsCount = rescueChopperFlights.count();

        // Save flights
        saveAsCsv(helicopterFlights, outputPath);

        // Print statistics
        List<String> statistics = new ArrayList<>();
        statistics.add(numberOfItemsStatistic("definite helicopters", rotorcraftCount));
        statistics.add(numberOfItemsStatistic("definite helicopter flights", rotorcraftFlightsCount));
        statistics.add(numberOfItemsStatistic("definite helicopters with flights", rotorcraftWithFlightsCount));
        statistics.add(numberOfItemsStatistic("other aircraft", otherAircraftCount));
        statistics.add(numberOfItemsStatistic("other aircraft flights", otherFlightsCount));
        statistics.add(numberOfItemsStatistic("other aircraft with flights", otherAircraftWithFlightsCount));
        statistics.add(numberOfItemsStatistic("other aircraft flights with altitude", otherFlightsWithAltitudeCount));
        statistics.add(numberOfItemsStatistic("other aircraft with flights with altitude", otherAircraftWithAltitudeCount));
        statistics.add(numberOfItemsStatistic("likely helicopters", likelyHelicoptersCount, otherAircraftCount));
        statistics.add(numberOfItemsStatistic("likely helicopter flights", likelyHelicopterFlightsCount));
        statistics.add(numberOfItemsStatistic("final helicopters", helicopterCount));
        statistics.add(numberOfItemsStatistic("final helicopter flights", helicopterFlightsCount));
        statistics.add(numberOfItemsStatistic("rescue helicopters", rescueChopperCount));
        statistics.add(numberOfItemsStatistic("rescue helicopter flights", rescueChopperFlightsCount));
        saveStatisticsAsTextFile(sc, outputPath, statistics);
    }


}
