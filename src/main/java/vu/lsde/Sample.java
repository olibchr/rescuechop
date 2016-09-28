package vu.lsde;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.*;


/**
 * Created by oliverbecher on 9/28/16.
 */

public class Sample {

    public static void main(String[] args) {

        String messages = "hdfs://hathi.surfsara.nl/user/hannes/opensky";

        SparkConf config = new SparkConf()
                .setAppName("Simple Application")
                .setMaster("local[*]");

        JavaSparkContext sc = new JavaSparkContext(config);

        SQLContext sqlContext = new SQLContext(sc);

        // Creates a DataFrame from a file
        DataFrame df = sqlContext.read().format("com.databricks.spark.avro").load(messages);

        DataFrame rawMessages = df.filter(col("rawMessage").like(""));

        rawMessages.write().format("com.databricks.spark.avro").save("dest");

    }

}
