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
public class Sampler {

    public static void main(String[] args) throws IOException {
        String inputPath = args[0];
        String outputPath = args[1];

        SparkConf sparkConf = new SparkConf().setAppName("LSDE09 Sampler");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        // Load records
        JavaRDD<GenericRecord> records = SparkAvroReader.loadJavaRDD(sc, inputPath, Config.OPEN_SKY_SCHEMA);

        // Map to model
        JavaRDD<SensorDatum> sensorData = records.map(new Function<GenericRecord, SensorDatum>() {
            public SensorDatum call(GenericRecord genericRecord) throws Exception {
                return SensorDatum.fromGenericRecord(genericRecord);
            }
        });//.persist(StorageLevel.MEMORY_AND_DISK());
        long inputRecordsCount = -1; //sensorData.count();

        // Filter out invalid messages
        sensorData = sensorData.filter(new Function<SensorDatum, Boolean>() {
            public Boolean call(SensorDatum sensorDatum) throws Exception {
                return sensorDatum.isValidMessage();
            }
        });//.persist(StorageLevel.MEMORY_AND_DISK());
        long validRecordsCount = -1; //sensorData.count();

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
        });//.persist(StorageLevel.MEMORY_AND_DISK());
        long usefulRecordsCount = -1;//sensorData.count();

        // Group models by icao
        JavaPairRDD<String, Iterable<SensorDatum>> sensorDataByAircraft = sensorData.groupBy(new Function<SensorDatum, String>() {
            public String call(SensorDatum sensorDatum) {
                return sensorDatum.getIcao();
            }
        });//.persist(StorageLevel.MEMORY_AND_DISK());
        long aircraftCount = -1;//sensorDataByAircraft.count();

        // Find aircraft flying lower than 3km
        JavaPairRDD<String, Iterable<SensorDatum>> possibleHelicopters = sensorDataByAircraft.filter(new Function<Tuple2<String, Iterable<SensorDatum>>, Boolean>() {
            public Boolean call(Tuple2<String, Iterable<SensorDatum>> tuple) throws Exception {
                for (SensorDatum sd: tuple._2) {
                    if (sd.getDecodedMessage() instanceof AltitudeReply) {
                        AltitudeReply msg = (AltitudeReply) sd.getDecodedMessage();
                        if (msg.getAltitude() != null && msg.getAltitude() > 3000) {
                            return false;
                        }
                    } else if (sd.getDecodedMessage() instanceof CommBAltitudeReply) {
                        CommBAltitudeReply msg = (CommBAltitudeReply) sd.getDecodedMessage();
                        if (msg.getAltitude() != null && msg.getAltitude() > 3000) {
                            return false;
                        }
                    } else if (sd.getDecodedMessage() instanceof AirbornePositionMsg) {
                        AirbornePositionMsg msg = (AirbornePositionMsg) sd.getDecodedMessage();
                        if (msg.hasAltitude() && msg.getAltitude() > 3000) {
                            return false;
                        }
                    } else if (sd.getDecodedMessage() instanceof AirspeedHeadingMsg) {
                        AirspeedHeadingMsg msg = (AirspeedHeadingMsg) sd.getDecodedMessage();
                        if (msg.hasAirspeedInfo() && msg.getAirspeed() > 90) {
                            return false;
                        }
                    } else if (sd.getDecodedMessage() instanceof VelocityOverGroundMsg) {
                        VelocityOverGroundMsg msg = (VelocityOverGroundMsg) sd.getDecodedMessage();
                        if (msg.hasVelocityInfo() && msg.getVelocity() > 90) {
                            return false;
                        }
                    } else if (sd.getDecodedMessage() instanceof IdentificationMsg) {
                        IdentificationMsg msg = (IdentificationMsg) sd.getDecodedMessage();
                        if (msg.getEmitterCategory() != 0 && !msg.getCategoryDescription().equals("Rotorcraft")) {
                            return false;
                        }
                    } else if (sd.getDecodedMessage() instanceof MilitaryExtendedSquitter) {
                        return false;
                    }
                }
                return true;
            }
        });//.cache();
        long potentialHelicoptersCount = -1;//possibleHelicopters.count();

        // Flatten
        JavaRDD<SensorDatum> sample = Transformations.flatten(possibleHelicopters);//.cache();
        long outputRecordsCount = -1;//sample.count();

        // To CSV
        Transformations.saveAsCsv(sample, outputPath);

        // Print statistics
        List<String> statistics = new ArrayList<String>();
        statistics.add(numberOfItemsStatistic("raw records             ", inputRecordsCount));
        statistics.add(numberOfItemsStatistic("valid records           ", validRecordsCount));
        statistics.add(numberOfItemsStatistic("useful records          ", usefulRecordsCount));
        statistics.add(numberOfItemsStatistic("unique aircraft         ", aircraftCount));
        statistics.add(numberOfItemsStatistic("potential helicopters   ", potentialHelicoptersCount));
        statistics.add(numberOfItemsStatistic("messages in final sample", outputRecordsCount));
        JavaRDD<String> statsRDD = sc.parallelize(statistics, 1);
        statsRDD.saveAsTextFile(outputPath + "_stats");
    }

    private static String numberOfItemsStatistic(String itemName, long count) {
        return String.format("Number of %s: %d", itemName, count);
    }
}
