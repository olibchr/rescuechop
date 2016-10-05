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
import org.opensky.libadsb.Position;
import scala.Tuple2;
import vu.lsde.core.model.SensorDatum;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class AircraftPositions {

    public static void main(String[] args) throws IOException {
        Logger log = LogManager.getLogger(AircraftPositions.class);
        log.setLevel(Level.INFO);

        String inputPath = args[0];
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

        // Group models by icao
        JavaPairRDD<String, Iterable<SensorDatum>> sensorDataByAircraft = sensorData.groupBy(new Function<SensorDatum, String>() {
            public String call(SensorDatum sensorDatum) {
                return sensorDatum.icao;
            }
        });

        // Map messages to positions
        JavaPairRDD<String, Iterable<Position>> positionsByAircraft = sensorDataByAircraft.flatMapValues(new Function<Iterable<SensorDatum>, Iterable<Iterable<Position>>>() {
            public Iterable<Iterable<Position>> call(Iterable<SensorDatum> sensorData) throws Exception {
                List<SensorDatum> sensorDataList = Lists.newArrayList(sensorData);
            }
        });



        // To file
        records.saveAsTextFile(outputPath);
    }
}
