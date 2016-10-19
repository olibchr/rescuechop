package vu.lsde.jobs;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.opensky.libadsb.msgs.IdentificationMsg;
import org.opensky.libadsb.msgs.ModeSReply;
import scala.Tuple2;
import vu.lsde.core.model.Flight;
import vu.lsde.core.model.FlightDatum;
import vu.lsde.core.model.SensorDatum;

import java.io.IOException;

import static vu.lsde.jobs.functions.FlightDataFunctions.sensorDataByAircraftToFlightDataByAircraft;
import static vu.lsde.jobs.functions.FlightFunctions.*;
import static vu.lsde.jobs.functions.FlightFunctions.splitflightsOnLandingAndLiftoff;
import static vu.lsde.jobs.functions.SensorDataFunctions.flightDataSensorData;
import static vu.lsde.jobs.functions.SensorDataFunctions.usefulSensorData;
import static vu.lsde.jobs.functions.SensorDataFunctions.validSensorData;

/**
 * Takes a avro files containing sensor data as input, and then creates two sets of data: one with definite airplanes and
 * one with definite helicopters. These can then be used to evaluate our classifiers.
 */
public class EvaluationSampler extends JobBase {

    public static void main(String[] args) throws IOException {
        String inputPath = args[0];
        String outputPath = args[1];

        SparkConf sparkConf = new SparkConf().setAppName("LSDE09 Sampler")
                .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                .registerKryoClasses(new Class[]{SensorDatum.class});
//                .set("spark.core.connection.ack.wait.timeout", "600s");

        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        // Load sensor data
        JavaRDD<SensorDatum> sensorData = readSensorDataAvro(sc, inputPath);

        // Filter out invalid messages
        sensorData = sensorData.filter(validSensorData());

        // Filter out messages we won't use anyway
        sensorData = sensorData.filter(usefulSensorData());

        // Group by icao
        JavaPairRDD<String, Iterable<SensorDatum>> sensorDataByAircraft = groupByIcao(sensorData);

        // Split on helis and planes
        JavaPairRDD<String, Iterable<SensorDatum>> sensorDataByHelis = sensorDataByAircraft.filter(new Function<Tuple2<String, Iterable<SensorDatum>>, Boolean>() {
            @Override
            public Boolean call(Tuple2<String, Iterable<SensorDatum>> t) throws Exception {
                for (SensorDatum sd : t._2) {
                    ModeSReply msg = sd.getDecodedMessage();
                    if (msg instanceof IdentificationMsg) {
                        IdentificationMsg idMsg = (IdentificationMsg) msg;
                        return idMsg.getCategoryDescription().equals("Rotorcraft");
                    }
                }
                return false;
            }
        });
        JavaPairRDD<String, Iterable<SensorDatum>> sensorDataByAirplanes = sensorDataByAircraft.filter(new Function<Tuple2<String, Iterable<SensorDatum>>, Boolean>() {
            @Override
            public Boolean call(Tuple2<String, Iterable<SensorDatum>> t) throws Exception {
                for (SensorDatum sd : t._2) {
                    ModeSReply msg = sd.getDecodedMessage();
                    if (msg instanceof IdentificationMsg) {
                        IdentificationMsg idMsg = (IdentificationMsg) msg;
                        return idMsg.getEmitterCategory() != 0 && !idMsg.getCategoryDescription().equals("Rotorcraft");
                    }
                }
                return false;
            }
        });

        // Flatten
        JavaRDD<SensorDatum> heliSensorData = flatten(sensorDataByHelis);
        JavaRDD<SensorDatum> planeSensorData = flatten(sensorDataByAirplanes);

        // Filter out irrelevant messages
        heliSensorData = heliSensorData.filter(flightDataSensorData());
        planeSensorData = planeSensorData.filter(flightDataSensorData());

        // Group by icao
        JavaPairRDD<String, Iterable<SensorDatum>> heliSensorDataByHeli = groupByIcao(heliSensorData);
        JavaPairRDD<String, Iterable<SensorDatum>> planeSensorDataByPlane = groupByIcao(planeSensorData);

        // Map to flight data
        JavaPairRDD<String, Iterable<FlightDatum>> heliFlightDataByHeli = heliSensorDataByHeli.mapToPair(sensorDataByAircraftToFlightDataByAircraft());
        JavaPairRDD<String, Iterable<FlightDatum>> planeFlightDataByPlane = planeSensorDataByPlane.mapToPair(sensorDataByAircraftToFlightDataByAircraft());

        // Filter on speed and altitude
        final Function<Tuple2<String, Iterable<FlightDatum>>, Boolean> speedAndAltitude = new Function<Tuple2<String, Iterable<FlightDatum>>, Boolean>() {
            public Boolean call(Tuple2<String, Iterable<FlightDatum>> t) throws Exception {
                boolean hasData = false;
                for (FlightDatum fd : t._2) {
                    hasData = true;
                    if (fd.hasAltitude() && fd.getAltitude() > 3000)
                        return false;
                    if (fd.hasVelocity() && fd.getVelocity() > 120)
                        return false;
                }
                return hasData;
            }
        };
        heliFlightDataByHeli = heliFlightDataByHeli.filter(speedAndAltitude);
        planeFlightDataByPlane = planeFlightDataByPlane.filter(speedAndAltitude);

        // Map to flights (splitting on time)
        JavaRDD<Flight> heliFlights = flatten(heliFlightDataByHeli.mapToPair(splitFlightDataOnTime()));
        JavaRDD<Flight> planeFlights = flatten(planeFlightDataByPlane.mapToPair(splitFlightDataOnTime()));

        // Filter out short flights, split remaining flights on altitude or distance, filter again on short flights
        heliFlights = heliFlights.filter(noShortFlight()).filter(hasPositionData())
                .flatMap(splitflightsOnLandingAndLiftoff()).filter(noShortFlight());
        planeFlights = planeFlights.filter(noShortFlight()).filter(hasPositionData())
                .flatMap(splitflightsOnLandingAndLiftoff()).filter(noShortFlight());

        // To CSV
        saveAsCsv(heliFlights, outputPath + "_heli_flights");
        saveAsCsv(planeFlights, outputPath + "_plane_flights");

        // Print statistics
//        List<String> statistics = new ArrayList<>();
//        saveStatisticsAsTextFile(sc, outputPath, statistics);
    }
}
