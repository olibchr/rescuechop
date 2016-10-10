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
import vu.lsde.core.model.AircraftPosition;
import vu.lsde.core.model.FlightDatum;
import vu.lsde.core.model.SensorDatum;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

/**
 * Maps sensor data to aircraft positions.
 */
public class FlightData {

    public static void main(String[] args) throws IOException {
        Logger log = LogManager.getLogger(FlightData.class);
        log.setLevel(Level.INFO);

        String inputPath = args[0];
        String outputPath = args[1];

        SparkConf sparkConf = new SparkConf().setAppName("LSDE09 FlightData");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        // Load CSV
        JavaRDD<String> records = sc.textFile(inputPath);

        // Parse CSV
        JavaRDD<SensorDatum> sensorData = records.map(new Function<String, SensorDatum>() {
            public SensorDatum call(String csv) throws Exception {
                return SensorDatum.fromCSV(csv);
            }
        });

        // Filter position and velocity messages
        sensorData = sensorData.filter(new Function<SensorDatum, Boolean>() {
            public Boolean call(SensorDatum sd) {
                return sd.getDecodedMessage() instanceof AirbornePositionMsg
                        || sd.getDecodedMessage() instanceof SurfacePositionMsg
                        || sd.getDecodedMessage() instanceof AirspeedHeadingMsg
                        || sd.getDecodedMessage() instanceof VelocityOverGroundMsg;
            }
        });

        // Group models by icao
        JavaPairRDD<String, Iterable<SensorDatum>> sensorDataByAircraft = sensorData.groupBy(new Function<SensorDatum, String>() {
            public String call(SensorDatum sensorDatum) {
                return sensorDatum.getIcao();
            }
        });

        // Map messages to flight data
        JavaPairRDD<String, Iterable<FlightDatum>> flightDataByAircraft = sensorDataByAircraft.mapToPair(new PairFunction<Tuple2<String, Iterable<SensorDatum>>, String, Iterable<FlightDatum>>() {
            public Tuple2<String, Iterable<FlightDatum>> call(Tuple2<String, Iterable<SensorDatum>> tuple) {
                String icao = tuple._1;
                Iterable<SensorDatum> sensorData = tuple._2;

                // Sort sensor data on time received for PositionDecoder
                List<SensorDatum> sensorDataList = Lists.newArrayList(sensorData);
                Collections.sort(sensorDataList);

                // Decode positions
                PositionDecoder decoder = new PositionDecoder();
                List<FlightDatum> flightData = new ArrayList<FlightDatum>();
                for (SensorDatum sd : sensorDataList) {
                    ModeSReply message = sd.getDecodedMessage();

                    if (message instanceof AirbornePositionMsg || message instanceof SurfacePositionMsg ) {
                        Position position = null;
                        Position sensorPosition = new Position(sd.getSensorLongitude(), sd.getSensorLatitude(), null);

                        if (message instanceof AirbornePositionMsg) {
                            position = decoder.decodePosition(sd.getTimeAtServer(), sensorPosition, (AirbornePositionMsg) message);
                        } else {
                            position = decoder.decodePosition(sd.getTimeAtServer(), sensorPosition, (SurfacePositionMsg) message);
                        }
                        if (position != null && position.isReasonable()) {
                            flightData.add(new FlightDatum(icao, sd.getTimeAtServer(), position));
                        }
                    } else {
                        if (message instanceof AirspeedHeadingMsg) {
                            flightData.add(new FlightDatum(icao, sd.getTimeAtServer(), (AirspeedHeadingMsg) message));
                        } else {
                            flightData.add(new FlightDatum(icao, sd.getTimeAtServer(), (VelocityOverGroundMsg) message));
                        }
                    }

                }
                return new Tuple2<String, Iterable<FlightDatum>>(icao, flightData);
            }
        });

        // Filter out vehicles that are always at zero altitude, or that don't have any position data
        flightDataByAircraft = flightDataByAircraft.filter(new Function<Tuple2<String, Iterable<FlightDatum>>, Boolean>() {
            public Boolean call(Tuple2<String, Iterable<FlightDatum>> tuple) throws Exception {
                for (FlightDatum fd : tuple._2) {
                    if (fd.getAltitude() != null) {
                        if (fd.getAltitude() > 0) {
                            return true;
                        }
                    } else if (fd.getLatitude() != null) {
                        // if some of the position data just didn't include altitude data, then we can't say for sure the
                        // vehicle was always on the ground, and so we leave it in
                        return true;
                    }
                }
                return false;
            }
        });

        // Flatten
        JavaRDD<FlightDatum> flightData = flightDataByAircraft.flatMap(new FlatMapFunction<Tuple2<String, Iterable<FlightDatum>>, FlightDatum>() {
            public Iterable<FlightDatum> call(Tuple2<String, Iterable<FlightDatum>> t) throws Exception {
                return t._2;
            }
        });

        // To CSV
        JavaRDD<String> flightDataCSV = flightData.map(new Function<FlightDatum, String>() {
            public String call(FlightDatum fd) {
                return fd.toCSV();
            }
        });

        // To file
        flightDataCSV.saveAsTextFile(outputPath);
    }
}
