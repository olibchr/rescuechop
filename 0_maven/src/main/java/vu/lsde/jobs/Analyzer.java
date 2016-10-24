package vu.lsde.jobs;

import org.apache.hadoop.io.NullWritable;
import org.apache.spark.Accumulator;
import org.apache.spark.AccumulatorParam;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.broadcast.Broadcast;
import org.opensky.libadsb.Position;
import org.opensky.libadsb.msgs.*;
import scala.Tuple2;
import vu.lsde.core.model.FlightDatum;
import vu.lsde.core.model.SensorDatum;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.*;

import static vu.lsde.jobs.functions.FlightDataFunctions.*;
import static vu.lsde.jobs.functions.SensorDataFunctions.*;

/**
 * Reads sensor data in avro format and analyzes the kind of messages that are in there.
 */
public class Analyzer extends JobBase {

    public static void main(String[] args) throws IOException {
        String inputPath = args[0];
        String outputPath = args[1];

        SparkConf sparkConf = new SparkConf().setAppName("LSDE09 Analyzer");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        // Load sensor data
        JavaRDD<SensorDatum> sensorData = readSensorDataAvro(sc, inputPath).cache();
        long recordsCount = sensorData.count();

        //
        //  MESSAGE TYPE DISTRIBUTION
        //

        // Accumulators for counting
        final Map<ModeSReply.subtype, Accumulator<Long>> accumulatorsByType = accumulatorsByMessageType(sc);
        final Accumulator<Long> invalidAcc = newLongAccumulator(sc);

        // Count message types
        sensorData.foreach(new VoidFunction<SensorDatum>() {
            public void call(SensorDatum sd) {
                ModeSReply msg = sd.getDecodedMessage();
                if (!sd.isValidMessage()) {
                    invalidAcc.add(1L);
                } else {
                    accumulatorsByType.get(msg.getType()).add(1L);
                }
            }
        });

        //
        // MESSAGE TIME DISTRIBUTION
        //

        // Count messages by day
        JavaPairRDD<String, Long> daysCount = sensorData
                .mapToPair(toDayCountTuple())
                .reduceByKey(add())
                .sortByKey()
                .mapToPair(formatUnixTime());

        //
        //  ROTORCRAFTS
        //

        // Get rotorcraft icaos
        List<String> rotorcraftIcaosList = sensorData
                .filter(validSensorData())
                .filter(identificationSensorData())
                .filter(rotorcraftSensorData())
                .map(JobBase.<SensorDatum>modelGetIcao())
                .distinct()
                .collect();

        final Broadcast<HashSet<String>> rotorcraftIcaos = sc.broadcast(new HashSet<>(rotorcraftIcaosList));

        // Get sensor data for rotorcrafts
        JavaPairRDD<String, Iterable<SensorDatum>> sensorDataByRotorcraft = sensorData
                .filter(new Function<SensorDatum, Boolean>() {
                    @Override
                    public Boolean call(SensorDatum sensorDatum) throws Exception {
                        return rotorcraftIcaos.getValue().contains(sensorDatum.getIcao());
                    }
                })
                .groupBy(JobBase.<SensorDatum>modelGetIcao());

        // Get flight data for rotorcrafts
        JavaPairRDD<String, Iterable<FlightDatum>> flightDataByRotorcraft = sensorDataByRotorcraft
                .mapToPair(sensorDataByAircraftToFlightDataByAircraft()).cache();

        // Find maximum speed and altitude for each helicopter
        JavaRDD<Tuple2<Double, Double>> rotorcraftMaxAltitudesVelocities = flightDataByRotorcraft
                .mapValues(flightDataToMaxAltitudeVelocityTuple())
                .values();

        // Flatten rotorcraft sensor data
        JavaRDD<FlightDatum> rotorcraftFlightData = flatten(flightDataByRotorcraft);

        // Get all altitudes for rotor craft
        JavaRDD<Double> rotorcraftAltitudes = rotorcraftFlightData
                .filter(altitudeFlightData())
                .map(flightDataGetAltitude());

        // Get all velocities for rotorcraft
        JavaRDD<Double> rotorcraftVelocities = rotorcraftFlightData
                .filter(velocityFlightData())
                .map(flightDataGetVelocity());


        //
        //  ALL AIRCRAFT
        //

        // Count all aircraft
        long aircraftCount = sensorData.map(JobBase.<SensorDatum>modelGetIcao()).distinct().count();

        // Sample flight data for all aircraft
        JavaRDD<FlightDatum> flightData = sensorData
                .sample(true, 0.001)
                .filter(validSensorData())
                .filter(usefulSensorData())
                .groupBy(JobBase.<SensorDatum>modelGetIcao())
                .mapToPair(sensorDataByAircraftToFlightDataByAircraft())
                .values()
                .flatMap(JobBase.<FlightDatum>flatten());

        // Find altitudes
        JavaRDD<Double> altitudes = flightData
                .filter(altitudeFlightData())
                .map(flightDataGetAltitude());

        // Find all velocities
        JavaRDD<Double> velocities = flightData
                .filter(velocityFlightData())
                .map(flightDataGetVelocity());

        // Find all positions
        JavaRDD<String> positionStrings = flightData
                .filter(positionFlightData())
                .map(flightDataToPositionStrings());

        // Save files
        altitudes.saveAsTextFile(outputPath + "_altitudes");
        velocities.saveAsTextFile(outputPath + "_velocities");
        positionStrings.saveAsTextFile(outputPath + "_positions");
        rotorcraftAltitudes.saveAsTextFile(outputPath + "_rc_altitudes");
        rotorcraftVelocities.saveAsTextFile(outputPath + "_rc_velocities");
        rotorcraftMaxAltitudesVelocities.map(doubleTupleToCsv()).saveAsTextFile(outputPath + "_rc_max_altitudes_velocities");

        // Print statistics
        List<String> statistics = new ArrayList<>();
        statistics.add(numberOfItemsStatistic("input records", recordsCount));
        statistics.add(numberOfItemsStatistic("aircraft", aircraftCount));
        for (ModeSReply.subtype subtype : accumulatorsByType.keySet()) {
            long count = accumulatorsByType.get(subtype).value();
            statistics.add(numberOfItemsStatistic(subtype.name(), count, recordsCount));
        }
        statistics.add(numberOfItemsStatistic("invalid", invalidAcc.value(), recordsCount));
        for (Tuple2<String, Long> t : daysCount.collect()) {
            statistics.add(numberOfItemsStatistic("data on " + t._1, t._2, recordsCount));
        }
        saveStatisticsAsTextFile(sc, outputPath, statistics);
    }

    private static Accumulator<Long> newLongAccumulator(JavaSparkContext sc) {
        return sc.accumulator(0L, new AccumulatorParam<Long>() {
            @Override
            public Long addAccumulator(Long aLong, Long t1) {
                return aLong + t1;
            }

            @Override
            public Long addInPlace(Long aLong, Long r1) {
                return aLong + r1;
            }

            @Override
            public Long zero(Long aLong) {
                return 0L;
            }
        });
    }

    private static Map<ModeSReply.subtype, Accumulator<Long>> accumulatorsByMessageType(JavaSparkContext sc) {
        Map<ModeSReply.subtype, Accumulator<Long>> accumulatorsByMessageType = new TreeMap<>();
        for (ModeSReply.subtype subtype : ModeSReply.subtype.values()) {
            accumulatorsByMessageType.put(subtype, newLongAccumulator(sc));
        }
        return accumulatorsByMessageType;
    }

    // FUNCTIONS

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

    private static Function<FlightDatum, Double> flightDataGetAltitude() {
        return new Function<FlightDatum, Double>() {
            @Override
            public Double call(FlightDatum flightDatum) throws Exception {
                return flightDatum.getAltitude();
            }
        };
    }

    private static Function<FlightDatum, Double> flightDataGetVelocity() {
        return new Function<FlightDatum, Double>() {
            @Override
            public Double call(FlightDatum flightDatum) throws Exception {
                return flightDatum.getVelocity();
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

    private static Function<Iterable<FlightDatum>, Tuple2<Double, Double>> flightDataToMaxAltitudeVelocityTuple() {
        return new Function<Iterable<FlightDatum>, Tuple2<Double, Double>>() {
            @Override
            public Tuple2<Double, Double> call(Iterable<FlightDatum> flightData) throws Exception {
                double maxAltitude = 0;
                double maxVelocity = 0;
                for (FlightDatum fd : flightData) {
                    double altitude = fd.hasAltitude() ? fd.getAltitude() : 0;
                    double velocity = fd.hasVelocity() ? fd.getVelocity() : 0;
                    maxAltitude = Math.max(maxAltitude, altitude);
                    maxVelocity = Math.max(maxVelocity, velocity);
                }
                return new Tuple2<>(maxAltitude, maxVelocity);
            }
        };
    }

    private static Function<Tuple2<Double, Double>, String> doubleTupleToCsv() {
        return new Function<Tuple2<Double, Double>, String>() {
            @Override
            public String call(Tuple2<Double, Double> t) throws Exception {
                return t._1.toString() + "," + t._2.toString();
            }
        };
    }

    private static PairFunction<String, String, String> icaoToIcaoIcaoPair() {
        return new PairFunction<String, String, String>() {
            @Override
            public Tuple2<String, String> call(String s) throws Exception {
                return new Tuple2<>(s, s);
            }
        };
    }
}
