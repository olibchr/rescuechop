package vu.lsde.core.model;

import com.google.common.base.Joiner;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericRecord;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.opensky.libadsb.Decoder;
import org.opensky.libadsb.Position;
import org.opensky.libadsb.exceptions.BadFormatException;
import org.opensky.libadsb.exceptions.UnspecifiedFormatError;
import org.opensky.libadsb.msgs.ModeSReply;
import org.opensky.libadsb.tools;

import java.io.Serializable;
import java.util.List;

/**
 * Immutable object representing one datum from a Mode S sensor.
 */
public class SensorDatum extends ModelBase {
    private static final Logger LOG = LogManager.getLogger(SensorDatum.class);
    private static final double NULL_DOUBLE = Double.MIN_VALUE;

    private final double sensorLatitude;
    private final double sensorLongitude;
    private final double timeAtServer;
    private final double timeAtSensor;
    private final double timestamp;
    private final String rawMessage;
    private final int sensorSerialNumber;

    private final ModeSReply decodedMessage;
    private final String icao;

    // CONSTRUCTOR

    public SensorDatum(Double sensorLatitude, Double sensorLongitude, double timeServer, Double timeSensor, Double timestamp, String rawMessage, int serialNumber) {

        if (rawMessage == null) throw new NullPointerException("rawMessage may not be null");

        this.sensorLatitude = sensorLatitude == null ? NULL_DOUBLE : sensorLatitude;
        this.sensorLongitude = sensorLongitude == null ? NULL_DOUBLE : sensorLongitude;
        this.timeAtServer = timeServer;
        this.timeAtSensor = timeSensor == null ? NULL_DOUBLE : timeSensor;
        this.timestamp = timestamp == null ? NULL_DOUBLE : timestamp;
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

    // GETTERS

    public Double getSensorLatitude() {
        return this.sensorLatitude != NULL_DOUBLE ? this.sensorLatitude : null;
    }

    public Double getSensorLongitude() {
        return this.sensorLongitude != NULL_DOUBLE ? this.sensorLongitude : null;
    }

    public double getTimeAtServer() {
        return this.timeAtServer;
    }

    public Double getTimeAtSensor() {
        return this.timeAtSensor != NULL_DOUBLE ? this.timeAtSensor : null;
    }

    public Double getTimestamp() {
        return this.timestamp != NULL_DOUBLE ? this.timestamp : null ;
    }

    public String getRawMessage() {
        return this.rawMessage;
    }

    public int getSensorSerialNumber() {
        return this.sensorSerialNumber;
    }

    public boolean isValidMessage() {
        return this.decodedMessage != null;
    }

    public ModeSReply getDecodedMessage() {
        return this.decodedMessage;
    }

    public String getIcao() {
        return this.icao;
    }

    // FUNCTIONS

    public String toCSV() {
        return super.toCSV(getSensorLatitude(), getSensorLongitude(), getTimeAtServer(), getTimeAtSensor(), getTimestamp(), getRawMessage(), getSensorSerialNumber());
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
                (Double) record.get("sensorLatitude"),
                (Double) record.get("sensorLongitude"),
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
        String[] tokens = csv.split(",");

        if (tokens.length < 6) throw new IllegalArgumentException(("CSV line for SensorDatum should consist of 7 columns"));

        double sensorLat = tokens[0].trim().length() == 0 ? -1 : Double.parseDouble(tokens[0]);
        double sensorLon = tokens[1].trim().length() == 0 ? -1 : Double.parseDouble(tokens[1]);
        double timeAtServer = tokens[2].trim().length() == 0 ? -1 : Double.parseDouble(tokens[2]);
        double timeAtSensor = tokens[3].trim().length() == 0 ? -1 : Double.parseDouble(tokens[3]);
        double timestamp = tokens[4].trim().length() == 0 ? -1 : Double.parseDouble(tokens[4]);
        String rawMessage = tokens[5];
        int sensorSerialNumber = tokens.length < 6 || tokens[6].trim().length() == 0 ? -1 : Integer.parseInt(tokens[6]);

        return new SensorDatum(sensorLat, sensorLon, timeAtServer, timeAtSensor, timestamp, rawMessage, sensorSerialNumber);
    }
}
