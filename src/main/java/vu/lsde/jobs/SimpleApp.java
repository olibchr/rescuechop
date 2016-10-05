package vu.lsde.jobs;

import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.*;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.Arrays;

/**
 * Hello world!
 *
 */
public class SimpleApp {
    public static void main(String[] args) {
        Logger log = LogManager.getRootLogger();
        log.setLevel(Level.INFO);

        log.info("test log 1");

        String path = "hdfs://hathi-surfsara/user/lsde09/test.text"; // Should be some file on your system
        SparkConf conf = new SparkConf()
                .setAppName("Simple Application");

        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> textFile = sc.textFile(path).cache();

        JavaRDD<String> words = textFile.flatMap(new FlatMapFunction<String, String>() {
            public Iterable<String> call(String s) {
                return Arrays.asList(s.split(" "));
            }
        });

        JavaPairRDD<String, Integer> pairs = words.mapToPair(new PairFunction<String, String, Integer>() {
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<String, Integer>(s, 1);
            }
        });

        JavaPairRDD<String, Integer> counts = pairs.reduceByKey(new Function2<Integer, Integer, Integer>() {
            public Integer call(Integer a, Integer b) throws Exception {
                return a + b;
            }
        });

        log.warn("test log 2");

        counts.saveAsTextFile("hdfs://hathi-surfsara/user/lsde09/test-output");
    }
}
