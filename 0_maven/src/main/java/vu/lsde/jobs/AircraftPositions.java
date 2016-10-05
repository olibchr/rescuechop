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
import org.opensky.libadsb.msgs.SurfacePositionMsg;
import scala.Tuple2;
import vu.lsde.core.Config;
import vu.lsde.core.model.SensorDatum;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

public class AircraftPositions {

    public static void main(String[] args) throws IOException {
        Logger log = LogManager.getLogger(AircraftPositions.class);
        log.setLevel(Level.INFO);

        String inputPath = Config.OPEN_SKY_SAMPLE_DATA_PATH;
        String outputPath = args[0];

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

        // Group models by icao
        JavaPairRDD<String, Iterable<SensorDatum>> sensorDataByAircraft = sensorData.groupBy(new Function<SensorDatum, String>() {
            public String call(SensorDatum sensorDatum) {
                return sensorDatum.icao;
            }
        });

        // Map messages to positions
        JavaPairRDD<String, Iterable<Position>> positionsByAircraft = sensorDataByAircraft.mapToPair(new PairFunction<Tuple2<String, Iterable<SensorDatum>>, String, Iterable<Position>>() {
            public Tuple2<String, Iterable<Position>> call(Tuple2<String, Iterable<SensorDatum>> tuple) throws Exception {
                Iterable<SensorDatum> sensorData = tuple._2;
                List<SensorDatum> sensorDataList = Lists.newArrayList(sensorData);
                Collections.sort(sensorDataList, new Comparator<SensorDatum>() {
                    public int compare(SensorDatum o1, SensorDatum o2) {
                        if (o1.timeAtServer < o2.timeAtServer) return -1;
                        if (o1.timeAtServer > o2.timeAtServer) return +1;
                        return 0;
                    }
                });

                PositionDecoder decoder = new PositionDecoder();
                List<Position> positions = new ArrayList<Position>();
                for (SensorDatum sd : sensorDataList) {
                    Position pos = null;
                    if (sd.decodedMessage instanceof AirbornePositionMsg) {
                        pos = decoder.decodePosition(sd.timeAtServer, (AirbornePositionMsg) sd.decodedMessage);
                    } else if (sd.decodedMessage instanceof SurfacePositionMsg) {
                        pos = decoder.decodePosition(sd.timeAtServer, (SurfacePositionMsg) sd.decodedMessage);
                    }
                    if (pos != null) {
                        positions.add(pos);
                    }
                }
                return new Tuple2<String, Iterable<Position>>(tuple._1, positions);
            }
        });

        // Flatten
        JavaPairRDD<String, Position> positions = positionsByAircraft.flatMapToPair(new PairFlatMapFunction<Tuple2<String, Iterable<Position>>, String, Position>() {
            public Iterable<Tuple2<String, Position>> call(Tuple2<String, Iterable<Position>> tuple) throws Exception {
                List<Tuple2<String, Position>> tupleList = new ArrayList<Tuple2<String, Position>>();

                for (Position position: tuple._2) {
                    tupleList.add(new Tuple2<String, Position>(tuple._1, position));
                }

                return tupleList;
            }
        });

        // To CSV
        JavaRDD<String> positionsCSV = positions.map(new Function<Tuple2<String, Position>, String>() {
            public String call(Tuple2<String, Position> tuple) throws Exception {
                Position p = tuple._2;
                return String.format("%s,%f,%f,%s", tuple._1, p.getLatitude(), p.getLongitude(), p.getAltitude() == null ? "" : p.getAltitude());
                // return Joiner.on(",").useForNull("").join(tuple._1, p.getLatitude(), p.getLongitude(), p.getAltitude());
            }
        });

        // To file
        positionsCSV.saveAsTextFile(outputPath);
    }
}
