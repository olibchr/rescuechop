package vu.lsde.jobs;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.opensky.libadsb.msgs.*;
import scala.Tuple2;
import vu.lsde.core.model.SensorDatum;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static vu.lsde.jobs.functions.SensorDataFunctions.rotorcraftSensorData;
import static vu.lsde.jobs.functions.SensorDataFunctions.usefulSensorData;
import static vu.lsde.jobs.functions.SensorDataFunctions.validSensorData;

/**
 * Job that takes sensor data as input, groups it by ICAO, and checks each aircraft for ADS-B identifcation messages.
 * Only sensor data that belongs to aircraft that specifically state they are a rotorcraft is left in.
 */
public class RotorcraftChecker extends JobBase {

    public static void main(String[] args) throws IOException {
        String inputPath = args[0];
        String outputPath = args[1];

        SparkConf sparkConf = new SparkConf().setAppName("LSDE09 RotorcraftChecker");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        // Load records
        JavaRDD<SensorDatum> sensorData = readSensorDataAvro(sc, inputPath);
        long recordsCount = sensorData.count();

        // Group models by icao
        JavaPairRDD<String, Iterable<SensorDatum>> sensorDataByAircraft = sensorData
                .filter(validSensorData())
                .filter(usefulSensorData())
                .groupBy(JobBase.<SensorDatum>modelGetIcao())
                .filter(rotorcraftSensorData());
        long rotorcraftCount = sensorDataByAircraft.count();

        // Find maximum speed and altitude
        Tuple2<Double, Double> maxAltitudeVelocity = flatten(sensorDataByAircraft)
                .mapToPair(sensorDatumToAltitudeVelocityTuple())
                .fold(zeroTuple(), maxAltitudeVelocityTuple());

        // Print statistics
        List<String> statistics = new ArrayList<String>();
        statistics.add(numberOfItemsStatistic("raw records", recordsCount));
        statistics.add(numberOfItemsStatistic("definite helicopters", rotorcraftCount));
        statistics.add("Maximum altitude: " + maxAltitudeVelocity._1);
        statistics.add("Maximum speed: " + maxAltitudeVelocity._2);
        saveStatisticsAsTextFile(sc, outputPath, statistics);
    }

    // FUNCTIONS

    private static PairFunction<SensorDatum, Double, Double> sensorDatumToAltitudeVelocityTuple() {
        return new PairFunction<SensorDatum, Double, Double>() {
            @Override
            public Tuple2<Double, Double> call(SensorDatum sd) throws Exception {
                ModeSReply modeSReply = sd.getDecodedMessage();
                double altitude = 0;
                double velocity = 0;
                if (modeSReply instanceof AirspeedHeadingMsg) {
                    AirspeedHeadingMsg msg = (AirspeedHeadingMsg) modeSReply;
                    velocity = msg.hasAirspeedInfo() ? msg.getAirspeed() : 0;
                } else if (modeSReply instanceof VelocityOverGroundMsg) {
                    VelocityOverGroundMsg msg = (VelocityOverGroundMsg) modeSReply;
                    velocity = msg.hasVelocityInfo() ? msg.getVelocity() : 0;
                } else if (modeSReply instanceof AirbornePositionMsg) {
                    AirbornePositionMsg msg = (AirbornePositionMsg) modeSReply;
                    altitude = msg.hasAltitude() ? msg.getAltitude() : 0;
                } else if (modeSReply instanceof AltitudeReply) {
                    AltitudeReply msg = (AltitudeReply) modeSReply;
                    altitude = msg.getAltitude() != null ? msg.getAltitude() : 0;
                } else if (modeSReply instanceof CommBAltitudeReply) {
                    CommBAltitudeReply msg = (CommBAltitudeReply) modeSReply;
                    altitude = msg.getAltitude() != null ? msg.getAltitude() : 0;
                }
                return new Tuple2<>(altitude, velocity);
            }
        };
    }

    private static Tuple2<Double, Double> zeroTuple() {
        return new Tuple2<>(0d, 0d);
    }

    private static Function2<Tuple2<Double, Double>, Tuple2<Double, Double>, Tuple2<Double, Double>> maxAltitudeVelocityTuple() {
        return new Function2<Tuple2<Double, Double>, Tuple2<Double, Double>, Tuple2<Double, Double>>() {
            @Override
            public Tuple2<Double, Double> call(Tuple2<Double, Double> t1, Tuple2<Double, Double> t2) throws Exception {
                return new Tuple2<>(Math.max(t1._1, t2._1), Math.max(t1._2, t2._2));
            }
        };
    }
}
