package vu.lsde.jobs;

import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import vu.lsde.core.model.SensorDatum;

import java.io.*;

/**
 * Converts binary sensor data to CSV sensor data.
 */
public class BinaryToCsv extends JobBase {

    public static void main(String[] args) throws IOException {
        String inputPath = args[0];
        String outputPath = args[1];

        SparkConf sparkConf = new SparkConf().setAppName("LSDE09 FlightData");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        // Get objects
        JavaRDD<SensorDatum> records = sc.objectFile(inputPath);

        // Save as CSV
        saveAsCsv(records, outputPath);
    }
}
