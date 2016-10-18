package vu.lsde.jobs;

import org.apache.spark.Accumulator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.opensky.libadsb.Position;
import org.opensky.libadsb.msgs.AirbornePositionMsg;
import org.opensky.libadsb.msgs.ModeSReply;
import org.opensky.libadsb.msgs.SurfacePositionMsg;
import scala.Tuple2;
import vu.lsde.core.model.FlightDatum;
import vu.lsde.core.model.SensorDatum;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.*;

import static vu.lsde.jobs.functions.FlightDataFunctions.sensorDataByAircraftToFlightDataByAircraft;

/**
 * Reads sensor data in CSV format and analyzes the kind of messages that are in there.
 */
public class MessageTypeAnalyzer extends JobBase {

    public static void main(String[] args) throws IOException {
        String inputPath = args[0];
        String outputPath = args[1];

        SparkConf sparkConf = new SparkConf().setAppName("LSDE09 MessageTypeAnalyzer");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        // Load sensor data
        JavaRDD<SensorDatum> sensorData = readSensorDataAvro(sc, inputPath).cache();
        long recordsCount = sensorData.count();

        // Accumulators for counting
        final Map<ModeSReply.subtype, Accumulator<Integer>> accumulatorsByType = accumulatorsByMessageType(sc);
        final Accumulator<Integer> invalidAcc = sc.accumulator(0);

        // Count message types
        sensorData.foreach(new VoidFunction<SensorDatum>() {
            public void call(SensorDatum sd) {
                ModeSReply msg = sd.getDecodedMessage();
                if (!sd.isValidMessage()) {
                    invalidAcc.add(1);
                } else {
                    accumulatorsByType.get(msg.getType()).add(1);
                }
            }
        });

        // Count messages by day
        JavaPairRDD<String, Long> daysCount = sensorData
                .mapToPair(toDayCountTuple())
                .reduceByKey(add())
                .sortByKey()
                .mapToPair(formatUnixTime());

        // Filter out irrelevant messages
        sensorData = sensorData.filter(positionSensorData());

        // Map to positions
        JavaPairRDD<String, Iterable<FlightDatum>> flightDataByAircraft = groupByIcao(sensorData).mapToPair(sensorDataByAircraftToFlightDataByAircraft());
        JavaRDD<String> positionStrings = flatten(flightDataByAircraft).map(flightDataToPositionStrings());

        // Save positions
        positionStrings.saveAsTextFile(outputPath + "_positions");

        // Print statistics
        List<String> statistics = new ArrayList<>();
        statistics.add(numberOfItemsStatistic("input records", recordsCount));
        for (ModeSReply.subtype subtype : accumulatorsByType.keySet()) {
            int count = accumulatorsByType.get(subtype).value();
            statistics.add(numberOfItemsStatistic(subtype.name(), count, recordsCount));
        }
        statistics.add(numberOfItemsStatistic("invalid", invalidAcc.value(), recordsCount));
        for (Tuple2<String, Long> t : daysCount.collect()) {
            statistics.add(numberOfItemsStatistic("data on " + t._1, t._2, recordsCount));
        }
        saveStatisticsAsTextFile(sc, outputPath, statistics);
    }

    private static Map<ModeSReply.subtype, Accumulator<Integer>> accumulatorsByMessageType(JavaSparkContext sc) {
        Map<ModeSReply.subtype, Accumulator<Integer>> accumulatorsByMessageType = new TreeMap<>();
        for (ModeSReply.subtype subtype : ModeSReply.subtype.values()) {
            accumulatorsByMessageType.put(subtype, sc.accumulator(0));
        }
        return accumulatorsByMessageType;
    }

    // FUNCTIONS

    private static Function<SensorDatum, Boolean> positionSensorData() {
        return new Function<SensorDatum, Boolean>() {
            @Override
            public Boolean call(SensorDatum sensorDatum) throws Exception {
                ModeSReply msg = sensorDatum.getDecodedMessage();
                return msg instanceof AirbornePositionMsg || msg instanceof SurfacePositionMsg;
            }
        };
    }

    private static PairFunction<SensorDatum, Long, Long> toDayCountTuple() {
        return new PairFunction<SensorDatum, Long, Long>() {
            @Override
            public Tuple2<Long, Long> call(SensorDatum sensorDatum) throws Exception {
                return new Tuple2<>(Math.round(sensorDatum.getTimeAtServer()) / 86400, 1L);
            }
        };
    }

    private static Function2<Long, Long, Long> add() {
        return new Function2<Long, Long, Long>() {
            @Override
            public Long call(Long a, Long b) throws Exception {
                return a + b;
            }
        };
    }

    private static Function<FlightDatum, String> flightDataToPositionStrings() {
        return new Function<FlightDatum, String>() {
            @Override
            public String call(FlightDatum flightDatum) throws Exception {
                Position position = flightDatum.getPosition();
                return position.getLatitude() + "," + position.getLongitude();
            }
        };
    }

    private static PairFunction<Tuple2<Long, Long>, String, Long> formatUnixTime() {
        return new PairFunction<Tuple2<Long, Long>, String, Long>() {
            @Override
            public Tuple2<String, Long> call(Tuple2<Long, Long> t) throws Exception {
                Date date = new Date();
                date.setTime(t._1 * 86400 * 1000);
                String prettyTime = new SimpleDateFormat("YYYY-MM-dd").format(date);
                return new Tuple2<>(prettyTime, t._2);
            }
        };
    }
}
