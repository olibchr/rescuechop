package vu.lsde.filter;

import org.apache.avro.generic.GenericRecord;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import scala.Tuple2;
import vu.lsde.core.Config;
import vu.lsde.core.model.SensorDatum;
import org.opensky.libadsb.msgs.VelocityOverGroundMsg;
import java.io.IOException;



/**
 * Created by oliverbecher on 10/3/16.
 */


public class Filter {
    public static void main(String[] args) throws IOException {

        String inputPath = "testSamplerOut3Part3.csv";
        String outputPath = args[0];

        SparkConf sparkConf = new SparkConf().setAppName("LSDE09 Filter");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        JavaRDD<String> csvData = sc.textFile(inputPath);

        JavaRDD<SensorDatum> rdd_records = sc.textFile(csvData).map(new Function <String, SensorDatum>(){
            public SensorDatum call(String line) throws Exception{
                String[] fields = line.split(",");
                SensorDatum sd = new SensorDatum(String(null), Double(null), Double(null), Double(null), double(fields[0]), double(fields[1]), double(fields[2]), String(fields[3]), int(fields[4]);

                return sd;
            }});


        JavaPairRDD<String, Iterable<SensorDatum>> sensorDataByAircraft = rdd_records.groupBy(new Function<SensorDatum, String>() {
            public String call(SensorDatum sensorDatum) {
                return sensorDatum.icao;
            }
        });

        // sort by timeatserver
        JavaPairRDD<String, Iterable<SensorDatum>>  sortedICAOs = sensorDataByAircraft.sortByKey(new Function<Iterable<SensorDatum>, String>() {
            public double call(SensorDatum sensorDatum) {
                return sensorDatum.timeAtServer;
            }
        });

        //check for movement greater than 300kmh (~=83m/s)
        JavaPairRDD<String, Iterable<SensorDatum>> HelisAreSlow = sortedICAOs.filter(new Function<Tuple2<String, Iterable<SensorDatum>>, Boolean>() {
            public Boolean call(Tuple2<String, Iterable<SensorDatum>> tuple) throws Exception {
                for (SensorDatum sd: tuple._2) {
                    if (sd.decodedMessage instanceof VelocityOverGroundMsg) {
                        if (((VelocityOverGroundMsg) sd.decodedMessage).getVelocity() > 83) {
                            return false;
                        }
                    }
                }
                return true;
            }
        });

        JavaRDD<SensorDatum> sample = sensorDataByAircraft.values().flatMap(new FlatMapFunction<Iterable<SensorDatum>, SensorDatum>() {
            public Iterable<SensorDatum> call(Iterable<SensorDatum> sensorData) throws Exception {
                return sensorData;
            }
        });

        // To CSV
        JavaRDD<String> sampleCSV = sample.map(new Function<SensorDatum, String>() {
            public String call(SensorDatum sensorDatum) throws Exception {
                return sensorDatum.toCSV();
            }
        });

        sampleCSV.saveAsTextFile(outputPath);
    }

}
