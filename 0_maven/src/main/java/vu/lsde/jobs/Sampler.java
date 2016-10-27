package vu.lsde.jobs;

import org.apache.avro.generic.GenericRecord;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.opensky.libadsb.msgs.*;
import scala.Tuple2;
import vu.lsde.core.Config;
import vu.lsde.core.io.SparkAvroReader;
import vu.lsde.core.model.SensorDatum;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static vu.lsde.jobs.functions.SensorDataFunctions.usefulSensorData;
import static vu.lsde.jobs.functions.SensorDataFunctions.validSensorData;

/**
 * Takes a avro files containing sensor data as input, and then filters out any sensor data belonging to aircraft that
 * are definitely NOT rotorcraft. This is done by looking at speed, altitude and identification. The remaining sensor
 * data is then output in the form of CSV.
 */
public class Sampler extends JobBase {

    private static final double MAX_ALTITUDE = 3500;
    private static final double MAX_VELOCITY = 100;

    public static void main(String[] args) throws IOException {
        String inputPath = args[0];
        String outputPath = args[1];

        SparkConf sparkConf = new SparkConf().setAppName("LSDE09 Sampler")
                .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                .registerKryoClasses(new Class[]{SensorDatum.class});
        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        // Get sensor data
        JavaRDD<SensorDatum> sensorData = readSensorDataAvro(sc, inputPath)
                .filter(validSensorData())
                .filter(usefulSensorData());

        // Map to pairs for aggregation
        JavaPairRDD<String, SensorDatum> pairs = sensorData.mapToPair(new PairFunction<SensorDatum, String, SensorDatum>() {
            public Tuple2<String, SensorDatum> call(SensorDatum sensorDatum) throws Exception {
                return new Tuple2<>(sensorDatum.getIcao(), sensorDatum);
            }
        });

        // For each partition, check if an aircraft is flying faster than MAX_VELOCITY or higher than MAX_ALTITUDE
        // If that is the case, return null. If not, return the list of sensor data for that aircraft.
        // The resulting lists are merged into one big list if none of the lists are null, otherwise null is returned.
        // This is basically the same as a group by and then filtering on MAX_ALTITUDE and MAX_DISTANCE, but should be
        // faster due to less shuffling.
        JavaPairRDD<String, List<SensorDatum>> sensorDataByAircraft = pairs.aggregateByKey(new ArrayList<SensorDatum>(), new Function2<List<SensorDatum>, SensorDatum, List<SensorDatum>>() {
            @Override
            public List<SensorDatum> call(List<SensorDatum> sensorData, SensorDatum sensorDatum) throws Exception {
                if (sensorData == null)
                    return null;

                ModeSReply decodedMessage = sensorDatum.getDecodedMessage();
                if (decodedMessage instanceof AltitudeReply) {
                    AltitudeReply msg = (AltitudeReply) decodedMessage;
                    if (msg.getAltitude() != null && msg.getAltitude() > MAX_ALTITUDE) {
                        sensorData = null;
                    }
                } else if (decodedMessage instanceof CommBAltitudeReply) {
                    CommBAltitudeReply msg = (CommBAltitudeReply) decodedMessage;
                    if (msg.getAltitude() != null && msg.getAltitude() > MAX_ALTITUDE) {
                        sensorData = null;
                    }
                } else if (decodedMessage instanceof AirbornePositionMsg) {
                    AirbornePositionMsg msg = (AirbornePositionMsg) decodedMessage;
                    if (msg.hasAltitude() && msg.getAltitude() > MAX_ALTITUDE) {
                        sensorData = null;
                    }
                } else if (decodedMessage instanceof AirspeedHeadingMsg) {
                    AirspeedHeadingMsg msg = (AirspeedHeadingMsg) decodedMessage;
                    if (msg.hasAirspeedInfo() && msg.getAirspeed() > MAX_VELOCITY) {
                        sensorData = null;
                    }
                } else if (decodedMessage instanceof VelocityOverGroundMsg) {
                    VelocityOverGroundMsg msg = (VelocityOverGroundMsg) decodedMessage;
                    if (msg.hasVelocityInfo() && msg.getVelocity() > MAX_VELOCITY) {
                        sensorData = null;
                    }
                } else if (decodedMessage instanceof IdentificationMsg) {
                    IdentificationMsg msg = (IdentificationMsg) decodedMessage;
                    if (msg.getEmitterCategory() != 0 && !msg.getCategoryDescription().equals("Rotorcraft")) {
                        sensorData = null;
                    }
                } else if (decodedMessage instanceof MilitaryExtendedSquitter) {
                    sensorData = null;
                }

                if (sensorData != null) {
                    sensorData.add(sensorDatum);
                }
                return sensorData;
            }
        }, new Function2<List<SensorDatum>, List<SensorDatum>, List<SensorDatum>>() {
            @Override
            public List<SensorDatum> call(List<SensorDatum> list1, List<SensorDatum> list2) throws Exception {
                List<SensorDatum> result = null;
                if (list1 != null && list2 != null) {
                    result = new ArrayList<>();
                    result.addAll(list1);
                    result.addAll(list2);
                }
                return result;
            }
        });

        // Filter out aircraft that have a null sensor data list, meaning that they're not helicopters. Also check whether
        // it has any airborne position messages. If not, we cannot use it (likely some ground vehicle)
        JavaPairRDD<String, List<SensorDatum>> sensorDataByPotentialHelis = sensorDataByAircraft.filter(new Function<Tuple2<String, List<SensorDatum>>, Boolean>() {
            @Override
            public Boolean call(Tuple2<String, List<SensorDatum>> t) throws Exception {
                if (t._2 != null) {
                    for (SensorDatum sd : t._2) {
                        ModeSReply decodedMessage = sd.getDecodedMessage();
                        if (decodedMessage instanceof AirbornePositionMsg) {
                            return true;
                        }
                    }
                }
                return false;
            }
        });

        // Flatten
        JavaRDD<SensorDatum> sample = flatten(sensorDataByPotentialHelis).cache();
        long outputRecordsCount = sample.count();

        // To CSV
        saveAsCsv(sample, outputPath);

        // Print statistics
        List<String> statistics = new ArrayList<>();
        statistics.add(numberOfItemsStatistic("messages in final sample", outputRecordsCount));
        saveStatisticsAsTextFile(sc, outputPath, statistics);
    }
}
