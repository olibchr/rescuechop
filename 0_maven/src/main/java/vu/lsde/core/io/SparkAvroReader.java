package vu.lsde.core.io;


import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyInputFormat;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import scala.Tuple2;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

public class SparkAvroReader {

    public static JavaRDD<GenericRecord> loadJavaRDD(JavaSparkContext sc, String inputPath, Schema schema) {
        Job job = getJob(schema);

        @SuppressWarnings("unchecked")
        JavaPairRDD<AvroKey<GenericRecord>, NullWritable> inputRecords = (JavaPairRDD<AvroKey<GenericRecord>, NullWritable>)
                sc.newAPIHadoopFile(inputPath, AvroKeyInputFormat.class, GenericRecord.class, NullWritable.class, job.getConfiguration());

        // Hadoop's RecordReader reuses the same Writable object for all records
        // which may lead to undesired behavior when caching RDD.
        // Cloning records solves this problem.
        JavaRDD<GenericRecord> result = inputRecords.map(new Function<Tuple2<AvroKey<GenericRecord>, NullWritable>, GenericRecord>() {
            public GenericRecord call(Tuple2<AvroKey<GenericRecord>, NullWritable> tuple) throws Exception {
                return cloneAvroRecord(tuple._1.datum());
            }
        });
        return result;
    }

    private static Job getJob(Schema schema) {
        Job job;

        try {
            job = Job.getInstance();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        AvroJob.setInputKeySchema(job, schema);

        return job;
    }

    @SuppressWarnings("unchecked")
    private static GenericRecord cloneAvroRecord(GenericRecord record) {
        GenericRecordBuilder builder = new GenericRecordBuilder((GenericData.Record) record);
        return builder.build();
    }
}
