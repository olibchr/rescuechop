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
import org.opensky.libadsb.msgs.AirbornePositionMsg;
import org.opensky.libadsb.msgs.IdentificationMsg;
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
 * Job that takes sensor data as input, groups it by ICAO, and checks each aircraft for ADS-B identifcation messages.
 * All sensor data belonging to aircrafts that explicitly broadcast they are NOT a rotorcraft, are discarded. Rotorcrafts
 * or unknowns are left in.
 */
public class IdentificationChecker {

    public static void main(String[] args) throws IOException {
        Logger log = LogManager.getLogger(IdentificationChecker.class);
        log.setLevel(Level.INFO);

        String inputPath = args[0]; //Config.OPEN_SKY_SAMPLE_DATA_PATH;
        String outputPath = args[1];

        SparkConf sparkConf = new SparkConf().setAppName("LSDE09 IdentificationChecker");
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
                return sensorDatum.getIcao();
            }
        });

        // Filter out all aircraft that are explicitly NOT a rotorcraft
        sensorDataByAircraft = sensorDataByAircraft.filter(new Function<Tuple2<String, Iterable<SensorDatum>>, Boolean>() {
            public Boolean call(Tuple2<String, Iterable<SensorDatum>> tuple) throws Exception {
                for (SensorDatum sd: tuple._2) {
                    if (sd.getDecodedMessage() instanceof IdentificationMsg) {
                        IdentificationMsg msg = (IdentificationMsg) sd.getDecodedMessage();
                        return msg.getEmitterCategory() == 0 // No information
                            || msg.getCategoryDescription().equals("Rotorcraft");
                    }
                }
                return true;
            }
        });

        // Flatten
        sensorData = sensorDataByAircraft.flatMap(new FlatMapFunction<Tuple2<String, Iterable<SensorDatum>>, SensorDatum>() {
            public Iterable<SensorDatum> call(Tuple2<String, Iterable<SensorDatum>> tuple) throws Exception {
                return tuple._2;
            }
        });

        // To CSV
        JavaRDD<String> sensorDataCSV = sensorData.map(new Function<SensorDatum, String>() {
            public String call(SensorDatum sensorDatum) throws Exception {
                return sensorDatum.toCSV();
            }
        });

        // To file
        sensorDataCSV.saveAsTextFile(outputPath);
    }
}
