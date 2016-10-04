package vu.lsde.filter;

import org.apache.avro.generic.GenericRecord;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import vu.lsde.core.model.SensorDatum;
import org.opensky.libadsb.msgs.VelocityOverGroundMsg;

/**
 * Created by oliverbecher on 10/3/16.
 */
public class Filter {
    public static void main(String[] args) throws IOException {
        SparkConf sparkConf = new SparkConf().setAppName("LSDE09 Filter");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        JavaRDD<String> csvData = sc.textFile("testSamplerOut3Part3.csv");

        JavaRDD<SensorDatum> rdd_records = sc.textFile(csvData).map(new Function <String, SensorDatum>(){
            public SensorDatum call(String line) throws Exception{
                String[] fields = line.split(",");
                SensorDatum sd = new SensorDatum(fields[0], fields[1], fields[2], fields[3], fields[4], fields[5], fields[6], fields[7], fields[8], fields[9], fields[10], fields[11], fields[12]);
                return sd;
            }});


        JavaPairRDD<String, Iterable<SensorDatum>> sensorDataByAircraft = sensorData.groupBy(new Function<SensorDatum, String>() {
            public String call(SensorDatum sensorDatum) {
                return sensorDatum.icao;
            }
        });

        // sort by timeatserver
        JavaPairRDD<String, Iterable<SensorDatum>>  sortedICAOs = sensorDataByAircraft.sortByKey(new Function<SensorDatum, String>() {
            public String call(SensorDatum sensorDatum) {
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
