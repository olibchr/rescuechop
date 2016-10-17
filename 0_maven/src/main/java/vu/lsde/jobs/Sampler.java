package vu.lsde.jobs;

import org.apache.avro.generic.GenericRecord;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.storage.StorageLevel;
import org.opensky.libadsb.msgs.*;
import scala.Tuple2;
import vu.lsde.core.Config;
import vu.lsde.core.io.SparkAvroReader;
import vu.lsde.core.model.SensorDatum;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Takes a avro files containing sensor data as input, and then filters out any sensor data belonging to aircraft that
 * are definitely NOT rotorcraft. This is done by looking at speed and altitude. The remaining sensordata is then output
 * in the form of CSV.
 */
public class Sampler extends JobBase {

    public static void main(String[] args) throws IOException {
        String inputPath = args[0];
        String outputPath = args[1];

        SparkConf sparkConf = new SparkConf().setAppName("LSDE09 Sampler")
                .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                .registerKryoClasses(new Class[]{SensorDatum.class});
//                .set("spark.core.connection.ack.wait.timeout", "600s");

        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        // Load records
        JavaRDD<GenericRecord> records = SparkAvroReader.loadJavaRDD(sc, inputPath, Config.OPEN_SKY_SCHEMA);

        // Map to model
        JavaRDD<SensorDatum> sensorData = records.map(new Function<GenericRecord, SensorDatum>() {
            public SensorDatum call(GenericRecord genericRecord) throws Exception {
                return SensorDatum.fromGenericRecord(genericRecord);
            }
        }).cache();
        long inputRecordsCount = sensorData.count();

        // Filter out invalid messages
        sensorData = sensorData.filter(new Function<SensorDatum, Boolean>() {
            public Boolean call(SensorDatum sensorDatum) throws Exception {
                return sensorDatum.isValidMessage();
            }
        }).cache();
        long validRecordsCount = sensorData.count();

        // Filter out messages we won't use anyway
        sensorData = sensorData.filter(new Function<SensorDatum, Boolean>() {
            public Boolean call(SensorDatum sensorDatum) throws Exception {
                switch(sensorDatum.getDecodedMessage().getType()) {
                    case MODES_REPLY:
                    case SHORT_ACAS:
                    case LONG_ACAS:
                    case EXTENDED_SQUITTER:
                    case COMM_D_ELM:
                    case ADSB_EMERGENCY:
                    case ADSB_TCAS:
                        return false;
                }
                return true;
            }
        }).cache();
        long usefulRecordsCount = sensorData.count();

        JavaPairRDD<String, SensorDatum> pairs = sensorData.mapToPair(new PairFunction<SensorDatum, String, SensorDatum>() {
            public Tuple2<String, SensorDatum> call(SensorDatum sensorDatum) throws Exception {
                return new Tuple2<>(sensorDatum.getIcao(), sensorDatum);
            }
        });

        JavaPairRDD<String, List<SensorDatum>> sensorDataByAircraft = pairs.aggregateByKey(new ArrayList<SensorDatum>(), new Function2<List<SensorDatum>, SensorDatum, List<SensorDatum>>() {
            @Override
            public List<SensorDatum> call(List<SensorDatum> sensorData, SensorDatum sensorDatum) throws Exception {
                if (sensorData == null)
                    return null;

                ModeSReply decodedMessage = sensorDatum.getDecodedMessage();
                if (decodedMessage instanceof AltitudeReply) {
                    AltitudeReply msg = (AltitudeReply) decodedMessage;
                    if (msg.getAltitude() != null && msg.getAltitude() > 3000) {
                        sensorData = null;
                    }
                } else if (decodedMessage instanceof CommBAltitudeReply) {
                    CommBAltitudeReply msg = (CommBAltitudeReply) decodedMessage;
                    if (msg.getAltitude() != null && msg.getAltitude() > 3000) {
                        sensorData = null;
                    }
                } else if (decodedMessage instanceof AirbornePositionMsg) {
                    AirbornePositionMsg msg = (AirbornePositionMsg) decodedMessage;
                    if (msg.hasAltitude() && msg.getAltitude() > 3000) {
                        sensorData = null;
                    }
                } else if (decodedMessage instanceof AirspeedHeadingMsg) {
                    AirspeedHeadingMsg msg = (AirspeedHeadingMsg) decodedMessage;
                    if (msg.hasAirspeedInfo() && msg.getAirspeed() > 120) {
                        sensorData = null;
                    }
                } else if (decodedMessage instanceof VelocityOverGroundMsg) {
                    VelocityOverGroundMsg msg = (VelocityOverGroundMsg) decodedMessage;
                    if (msg.hasVelocityInfo() && msg.getVelocity() > 120) {
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
        }).cache();
        long aircraftCount = sensorDataByAircraft.count();

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
        }).cache();
        long potentialHelicoptersCount = sensorDataByPotentialHelis.count();

        // Flatten
        JavaRDD<SensorDatum> sample = flatten(sensorDataByPotentialHelis).cache();
        long outputRecordsCount = sample.count();

        // To CSV
        saveAsCsv(sample, outputPath);

        // Print statistics
        List<String> statistics = new ArrayList<>();
        statistics.add(numberOfItemsStatistic("raw records             ", inputRecordsCount));
        statistics.add(numberOfItemsStatistic("valid records           ", validRecordsCount));
        statistics.add(numberOfItemsStatistic("useful records          ", usefulRecordsCount));
        statistics.add(numberOfItemsStatistic("unique aircraft         ", aircraftCount));
        statistics.add(numberOfItemsStatistic("potential helicopters   ", potentialHelicoptersCount));
        statistics.add(numberOfItemsStatistic("messages in final sample", outputRecordsCount));
        saveStatisticsAsTextFile(sc, outputPath, statistics);
    }
}
