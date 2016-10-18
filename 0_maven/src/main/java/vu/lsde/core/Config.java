package vu.lsde.core;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;

public class Config {
    /**
     * The base HDFS url for the cluster.
     */
    public static final String HDFS_BASE_PATH = "hdfs://hathi-surfsara/";

    /**
     * The path to the open sky dataset.
     */
    public static final String OPEN_SKY_DATA_PATH = HDFS_BASE_PATH + "user/hannesm/lsde/opensky/";

    /**
     * The path to the open sky sample dataset.
     */
    public static final String OPEN_SKY_SAMPLE_DATA_PATH = HDFS_BASE_PATH + "user/lsde09/opensky_sample";

    /**
     * The path to the helipad dataset.
     */
    public static final String HELIPAD_DATA_PATH = "4_data/europe_helipads.csv";

    /**
     * The AVRO schema for the open sky dataset.
     */
    public static final Schema OPEN_SKY_SCHEMA = SchemaBuilder
            .record("ModeSEncodedMessage").namespace("org.opensky.avro.v2")
            .fields()
            .name("sensorType").type().stringType().noDefault()
            .name("sensorLatitude").type().nullable().doubleType().noDefault()
            .name("sensorLongitude").type().nullable().doubleType().noDefault()
            .name("sensorAltitude").type().nullable().doubleType().noDefault()
            .name("timeAtServer").type().doubleType().noDefault()
            .name("timeAtSensor").type().nullable().doubleType().noDefault()
            .name("timestamp").type().nullable().doubleType().noDefault()
            .name("rawMessage").type().stringType().noDefault()
            .name("sensorSerialNumber").type().intType().noDefault()
            .name("RSSIPacket").type().nullable().doubleType().noDefault()
            .name("RSSIPreamble").type().nullable().doubleType().noDefault()
            .name("SNR").type().nullable().doubleType().noDefault()
            .name("confidence").type().nullable().doubleType().noDefault()
            .endRecord();
}
