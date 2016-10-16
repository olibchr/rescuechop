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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Maps sensor data to aircraft positions.
 */
public class FlightData {

    public static void main(String[] args) throws IOException {
        String inputPath = args[0];
        String outputPath = args[1];

        SparkConf sparkConf = new SparkConf().setAppName("LSDE09 FlightData");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        // Load sensor data
        JavaRDD<SensorDatum> sensorData = Transformations.readSensorDataCsv(sc, inputPath);
        long recordsCount = sensorData.count();

        // Filter position and velocity messages
        sensorData = sensorData.filter(new Function<SensorDatum, Boolean>() {
            public Boolean call(SensorDatum sd) {
                return sd.getDecodedMessage() instanceof AirbornePositionMsg
                    || sd.getDecodedMessage() instanceof SurfacePositionMsg
                    || sd.getDecodedMessage() instanceof AirspeedHeadingMsg
                    || sd.getDecodedMessage() instanceof VelocityOverGroundMsg
                    || sd.getDecodedMessage() instanceof AltitudeReply
                    || sd.getDecodedMessage() instanceof CommBAltitudeReply;
            }
        });
        long filteredRecordsCount = sensorData.count();

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
                List<FlightDatum> flightData = new ArrayList<>();
                for (SensorDatum sd : sensorDataList) {
                    ModeSReply message = sd.getDecodedMessage();

                    if (message instanceof AirbornePositionMsg || message instanceof SurfacePositionMsg ) {
                        Position position;
                        Position sensorPosition = new Position(sd.getSensorLongitude(), sd.getSensorLatitude(), null);

                        if (message instanceof AirbornePositionMsg) {
                            position = decoder.decodePosition(sd.getTimeAtServer(), sensorPosition, (AirbornePositionMsg) message);
                        } else {
                            position = decoder.decodePosition(sd.getTimeAtServer(), sensorPosition, (SurfacePositionMsg) message);
                        }
                        if (position != null && position.isReasonable()) {
                            flightData.add(new FlightDatum(icao, sd.getTimeAtServer(), position));
                        }
                    } else if (message instanceof AltitudeReply || message instanceof CommBAltitudeReply) {
                        Double altitude;
                        if (message instanceof AltitudeReply) {
                            altitude = ((AltitudeReply) message).getAltitude();
                        } else {
                            altitude = ((CommBAltitudeReply) message).getAltitude();
                        }
                        if (altitude != null) {
                            flightData.add(new FlightDatum(icao, sd.getTimeAtServer(), altitude));
                        }
                    } else if (message instanceof AirspeedHeadingMsg) {
                        flightData.add(new FlightDatum(icao, sd.getTimeAtServer(), (AirspeedHeadingMsg) message));
                    } else if (message instanceof VelocityOverGroundMsg) {
                        flightData.add(new FlightDatum(icao, sd.getTimeAtServer(), (VelocityOverGroundMsg) message));
                    }

                }
                return new Tuple2<String, Iterable<FlightDatum>>(icao, flightData);
            }
        });

        // Flatten
        JavaRDD<FlightDatum> flightData = Transformations.flatten(flightDataByAircraft).cache();

        // Write to CSV
        Transformations.saveAsCsv(flightData, outputPath);

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
        List<String> statistics = new ArrayList<String>();
        statistics.add(numberOfItemsStatistic("input records     ", recordsCount));
        statistics.add(numberOfItemsStatistic("filtered records  ", filteredRecordsCount));
        statistics.add(numberOfItemsStatistic("output flight data", flightDataCount));
        statistics.add(numberOfItemsStatistic("position data     ", positionDataCount, flightDataCount));
        statistics.add(numberOfItemsStatistic("altitude data     ", altitudeDataCount, flightDataCount));
        statistics.add(numberOfItemsStatistic("velocity data     ", velocityDataCount, flightDataCount));
        statistics.add(numberOfItemsStatistic("rate of climb data", rocDataCount, flightDataCount));
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
