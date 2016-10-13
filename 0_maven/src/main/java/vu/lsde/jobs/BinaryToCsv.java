package vu.lsde.jobs;

import org.apache.hadoop.fs.BufferedFSInputStream;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.input.PortableDataStream;
import vu.lsde.core.model.SensorDatum;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

/**
 * Converts binary sensor data to CSV sensor data.
 */
public class BinaryToCsv {

    public static void main(String[] args) throws IOException {
        Logger log = LogManager.getLogger(BinaryToCsv.class);
        log.setLevel(Level.INFO);

        String inputPath = args[0];
        String outputPath = args[1];

        SparkConf sparkConf = new SparkConf().setAppName("LSDE09 FlightData");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        // Get objects
        JavaRDD<SensorDatum> records = sc.objectFile(inputPath);

        // Map to CSV
        JavaRDD<String> csv = records.map(new Function<SensorDatum, String>() {
            public String call(SensorDatum sensorDatum) throws Exception {
                return sensorDatum.toCSV();
            }
        });

        // Write to file
        csv.saveAsTextFile(outputPath);
    }
}
