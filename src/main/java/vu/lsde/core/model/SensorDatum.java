package vu.lsde.core.model;

import org.apache.avro.generic.GenericRecord;
import org.opensky.libadsb.Decoder;
import org.opensky.libadsb.exceptions.BadFormatException;
import org.opensky.libadsb.exceptions.UnspecifiedFormatError;
import org.opensky.libadsb.msgs.ModeSReply;
import org.opensky.libadsb.tools;
import vu.lsde.core.io.CsvStringBuilder;

import java.io.Serializable;

/**
 * Immutable object representing one datum from a Mode S sensor.
 */
public class SensorDatum implements Serializable {
    public final double timeAtServer;
    public final double timeAtSensor;
    public final double timestamp;
    public final String rawMessage;
    public final int sensorSerialNumber;
    public final ModeSReply decodedMessage;
    public final String icao;

    public SensorDatum(double timeServer, Double timeSensor, Double timestamp, String rawMessage, int serialNumber) {
        if (rawMessage == null) throw new NullPointerException("rawMessage may not be null");

        this.timeAtServer = timeServer;
        this.timeAtSensor = timeSensor == null ? -1 : timeSensor;
        this.timestamp = timestamp == null ? -1 : timestamp;
        this.rawMessage = rawMessage;
        this.sensorSerialNumber = serialNumber;

        ModeSReply decodedMessage;
        try {
            decodedMessage = Decoder.genericDecoder(rawMessage);
        } catch (BadFormatException e) {
            decodedMessage = null;
        } catch (UnspecifiedFormatError e) {
            decodedMessage = null;
        } catch (ArrayIndexOutOfBoundsException e) {
            // Somehow this happens. I don't know why, but it happens. Log it and go on with life.
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
                .addValue(this.timeAtServer)
                .addValue(this.timeAtSensor)
                .addValue(this.timestamp)
                .addValue(this.rawMessage)
                .addValue(this.sensorSerialNumber)
                .toString();
    }

    // STATIC

    /**
     * Creates a SensorDatum object given a GenericRecord with the schema defined at
     * <a href="https://github.com/openskynetwork/osky-sample">github.com/openskynetwork/osky-sample</a>.
     *
     * @param record
     * @return
     */
    public static SensorDatum fromGenericRecord(GenericRecord record) {
        return new SensorDatum(
                (Double) record.get("timeAtServer"),
                (Double) record.get("timeAtSensor"),
                (Double) record.get("timestamp"),
                (String) record.get("rawMessage"),
                (Integer) record.get("sensorSerialNumber")
        );
    }
}
