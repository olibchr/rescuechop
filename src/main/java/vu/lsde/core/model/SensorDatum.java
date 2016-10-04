package vu.lsde.core.model;

import org.apache.avro.generic.GenericRecord;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
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
    private static final Logger LOG = LogManager.getLogger(SensorDatum.class);

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
            LOG.warn("could not decode raw message due to ArrayIndexOutOfBoundsException", e);
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

    /**
     * Creates a SensorDatum object given a CSV line. It should consist of five columns containing timeAtServer,
     * timeAtSensor, timestamp, rawMessage and sensorSerialNumber, in that order.
     *
     * @param csv
     * @return
     */
    public static SensorDatum fromCSV(String csv) {
        String tokens[] = csv.split(",");

        if (tokens.length < 5) throw new IllegalArgumentException("CSV line should consist of 5 columns");

        double timeAtServer = tokens[0].length() == 0 ? -1 : Double.parseDouble(tokens[0]);
        double timeAtSensor = tokens[1].length() == 0 ? -1 : Double.parseDouble(tokens[1]);
        double timestamp = tokens[2].length() == 0 ? -1 : Double.parseDouble(tokens[2]);
        String rawMessage = tokens[3];
        int sensorSerialNumber = tokens[4].length() == 0 ? -1 : Integer.parseInt(tokens[4]);

        return new SensorDatum(timeAtServer, timeAtSensor, timestamp, rawMessage, sensorSerialNumber);
    }
}
