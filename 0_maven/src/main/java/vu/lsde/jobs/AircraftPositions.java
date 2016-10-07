package vu.lsde.jobs;

import com.clearspring.analytics.util.Lists;
import com.google.common.base.Joiner;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.opensky.libadsb.Position;
import org.opensky.libadsb.PositionDecoder;
import org.opensky.libadsb.msgs.AirbornePositionMsg;
import org.opensky.libadsb.msgs.AltitudeReply;
import org.opensky.libadsb.msgs.ModeSReply;
import org.opensky.libadsb.msgs.SurfacePositionMsg;
import scala.Tuple2;
import vu.lsde.core.Config;
import vu.lsde.core.model.AircraftPosition;
import vu.lsde.core.model.SensorDatum;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

/**
 * Maps sensor data to aircraft positions.
 */
public class AircraftPositions {

    public static void main(String[] args) throws IOException {
        Logger log = LogManager.getLogger(AircraftPositions.class);
        log.setLevel(Level.INFO);

        String inputPath = args[0]; //Config.OPEN_SKY_SAMPLE_DATA_PATH;
        String outputPath = args[1];

        SparkConf sparkConf = new SparkConf().setAppName("LSDE09 AircraftPositions");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        // Load CSV
        JavaRDD<String> records = sc.textFile(inputPath);

        // Parse CSV
        JavaRDD<SensorDatum> sensorData = records.map(new Function<String, SensorDatum>() {
            public SensorDatum call(String csv) throws Exception {
                return SensorDatum.fromCSV(csv);
            }
        });

        // Filter position and altitude messages
        sensorData = sensorData.filter(new Function<SensorDatum, Boolean>() {
            public Boolean call(SensorDatum sd) {
                return sd.getDecodedMessage() instanceof AirbornePositionMsg
                        || sd.getDecodedMessage() instanceof SurfacePositionMsg;
//                        || sd.getDecodedMessage() instanceof AltitudeReply;
            }
        });

        // Group models by icao
        JavaPairRDD<String, Iterable<SensorDatum>> sensorDataByAircraft = sensorData.groupBy(new Function<SensorDatum, String>() {
            public String call(SensorDatum sensorDatum) {
                return sensorDatum.getIcao();
            }
        });

        // Map messages to positions
        JavaPairRDD<String, Iterable<AircraftPosition>> positionsByAircraft = sensorDataByAircraft.mapToPair(new PairFunction<Tuple2<String, Iterable<SensorDatum>>, String, Iterable<AircraftPosition>>() {
            public Tuple2<String, Iterable<AircraftPosition>> call(Tuple2<String, Iterable<SensorDatum>> tuple) {
                String icao = tuple._1;
                Iterable<SensorDatum> sensorData = tuple._2;

                // Sort sensor data on time received for PositionDecoder
                List<SensorDatum> sensorDataList = Lists.newArrayList(sensorData);
                Collections.sort(sensorDataList, new Comparator<SensorDatum>() {
                    public int compare(SensorDatum o1, SensorDatum o2) {
                        if (o1.getTimeAtServer() < o2.getTimeAtServer()) return -1;
                        if (o1.getTimeAtServer() > o2.getTimeAtServer()) return +1;
                        return 0;
                    }
                });

                // Decode positions
                PositionDecoder decoder = new PositionDecoder();
                List<AircraftPosition> positions = new ArrayList<AircraftPosition>();
//                double lastAltitude = Double.MIN_VALUE;
//                double lastAltitudeTime = Double.MIN_VALUE;
                for (SensorDatum sd : sensorDataList) {
                    Position position = null;
                    Position sensorPosition = new Position(sd.getSensorLongitude(), sd.getSensorLatitude(), null);
                    ModeSReply message = sd.getDecodedMessage();
                    if (message instanceof AirbornePositionMsg) {
                        position = decoder.decodePosition(sd.getTimeAtServer(), sensorPosition, (AirbornePositionMsg) message);
                    } else if (message instanceof SurfacePositionMsg) {
                        position = decoder.decodePosition(sd.getTimeAtServer(), sensorPosition, (SurfacePositionMsg) message);
                    }
//                    else if (message instanceof AltitudeReply) {
//                        lastAltitude = ((AltitudeReply) message).getAltitude();
//                        lastAltitudeTime = sd.getTimeAtServer();
//                    }
                    if (position != null && position.isReasonable()) {
//                        if (position.getAltitude() == null && lastAltitude > Double.MIN_VALUE) {
//                            if (sd.getTimeAtServer() - lastAltitudeTime > 1800) {
//                                lastAltitude = Double.MIN_VALUE;
//                            } else {
//                                lastAltitudeTime = sd.getTimeAtServer();
//                                position.setAltitude(lastAltitude);
//                            }
//                        } else if (position.getAltitude() != null) {
//                            lastAltitude = position.getAltitude();
//                            lastAltitudeTime = sd.getTimeAtServer();
//                        }
                        positions.add(new AircraftPosition(icao, sd.getTimeAtServer(), position));
                    }
                }
                return new Tuple2<String, Iterable<AircraftPosition>>(icao, positions);
            }
        });

        // Flatten
        JavaRDD<AircraftPosition> positions = positionsByAircraft.flatMap(new FlatMapFunction<Tuple2<String, Iterable<AircraftPosition>>, AircraftPosition>() {
            public Iterable<AircraftPosition> call(Tuple2<String, Iterable<AircraftPosition>> t) throws Exception {
                return t._2;
            }
        });

        long positionsCount = positions.count();
        long positionsWithAltitudeCount = positions.filter(new Function<AircraftPosition, Boolean>() {
            public Boolean call(AircraftPosition aircraftPosition) throws Exception {
                return aircraftPosition.hasAltitude();
            }
        }).count();

        log.info(String.format("positions found: %d", positionsCount));
        log.info(String.format("positions with altitude found: %d", positionsWithAltitudeCount));
        log.info(String.format("%2.2f%% percent of positions include altitude data", 100d * positionsWithAltitudeCount / positionsCount));

        // To CSV
        JavaRDD<String> positionsCSV = positions.map(new Function<AircraftPosition, String>() {
            public String call(AircraftPosition p) {
                return p.toCSV(true);
            }
        });

        // To file
        positionsCSV.saveAsTextFile(outputPath);
    }
}
