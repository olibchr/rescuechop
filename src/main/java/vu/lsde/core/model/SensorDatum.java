package vu.lsde.core.model;

import org.apache.avro.generic.GenericRecord;
import org.opensky.libadsb.Decoder;
import org.opensky.libadsb.exceptions.BadFormatException;
import org.opensky.libadsb.exceptions.UnspecifiedFormatError;
import org.opensky.libadsb.msgs.ModeSReply;
import org.opensky.libadsb.tools;
import vu.lsde.core.io.CsvStringBuilder;

import java.io.Serializable;

public class SensorDatum implements Serializable {
    public final String sensorType;
    public final Double sensorLatitude;
    public final Double sensorLongitude;
    public final Double sensorAltitude;
    public final double timeAtServer;
    public final Double timeAtSensor;
    public final Double timestamp;
    public final String rawMessage;
    public final int sensorSerialNumber;
    public final Double RSSIPacket;
    public final Double RSSIPreamble;
    public final Double SNR;
    public final Double confidence;
    public final ModeSReply decodedMessage;
    public final String icao;

    public SensorDatum(String type, Double lat, Double lon, Double alt, double timeServer, Double timeSensor,
                       Double timestamp, String rawMessage, int serialNumber, Double rssiPacket, Double rssiPreamble,
                       Double snr, Double confidence) {
        if (type == null) throw new NullPointerException("sensorType may not be null");
        if (rawMessage == null) throw new NullPointerException("rawMessage may not be null");

        this.sensorType = type;
        this.sensorLatitude = lat;
        this.sensorLongitude = lon;
        this.sensorAltitude = alt;
        this.timeAtServer = timeServer;
        this.timeAtSensor = timeSensor;
        this.timestamp = timestamp;
        this.rawMessage = rawMessage;
        this.sensorSerialNumber = serialNumber;
        this.RSSIPacket = rssiPacket;
        this.RSSIPreamble = rssiPreamble;
        this.SNR = snr;
        this.confidence = confidence;

        ModeSReply decodedMessage;
        try {
            decodedMessage = Decoder.genericDecoder(rawMessage);
        } catch (BadFormatException e) {
            decodedMessage = null;
        } catch (UnspecifiedFormatError e) {
            decodedMessage = null;
        }
        this.decodedMessage = decodedMessage;
        this.icao = decodedMessage != null ? tools.toHexString(decodedMessage.getIcao24()) : null;
    }

    public boolean isValidMessage() {
        return this.decodedMessage != null;
    }

    public String toCSV() {
        return new CsvStringBuilder()
                .addValue(this.sensorType)
                .addValue(this.sensorLatitude)
                .addValue(this.sensorLongitude)
                .addValue(this.sensorAltitude)
                .addValue(this.timeAtServer)
                .addValue(this.timeAtSensor)
                .addValue(this.timestamp)
                .addValue(this.rawMessage)
                .addValue(this.sensorSerialNumber)
                .addValue(this.RSSIPacket)
                .addValue(this.RSSIPreamble)
                .addValue(this.SNR)
                .addValue(this.confidence)
                .toString();
    }

    // STATIC

    public static SensorDatum fromGenericRecord(GenericRecord record) {
        return new SensorDatum(
                (String) record.get("sensorType"),
                (Double) record.get("sensorLatitude"),
                (Double) record.get("sensorLongitude"),
                (Double) record.get("sensorAltitude"),
                (Double) record.get("timeAtServer"),
                (Double) record.get("timeAtSensor"),
                (Double) record.get("timestamp"),
                (String) record.get("rawMessage"),
                (Integer) record.get("sensorSerialNumber"),
                (Double) record.get("RSSIPacket"),
                (Double) record.get("RSSIPreamble"),
                (Double) record.get("SNR"),
                (Double) record.get("confidence")
        );
    }
}
