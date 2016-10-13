package vu.lsde.core.model;

import org.apache.avro.generic.GenericRecord;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.opensky.libadsb.Decoder;
import org.opensky.libadsb.exceptions.BadFormatException;
import org.opensky.libadsb.exceptions.UnspecifiedFormatError;
import org.opensky.libadsb.msgs.ModeSReply;
import org.opensky.libadsb.tools;
import vu.lsde.core.io.CsvReader;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.List;

/**
 * Immutable object representing one datum from a Mode S sensor.
 */
public class SensorDatum extends ModelBase implements Comparable<SensorDatum> {
    private static final Logger LOG = LogManager.getLogger(SensorDatum.class);

    private double sensorLatitude;
    private double sensorLongitude;
    private double timeAtServer;
    private String rawMessage;

    private transient ModeSReply decodedMessage;
    private transient String icao;

    // CONSTRUCTOR

    public SensorDatum(Double sensorLatitude, Double sensorLongitude, double timeServer, String rawMessage) {

        if (rawMessage == null) throw new NullPointerException("rawMessage may not be null");

        this.sensorLatitude = nullToNullDouble(sensorLatitude);
        this.sensorLongitude = nullToNullDouble(sensorLongitude);
        this.timeAtServer = timeServer;
        this.rawMessage = rawMessage;

        init();
    }

    private void init() {
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
        return nullDoubleToNull(this.sensorLatitude);
    }

    public Double getSensorLongitude() {
        return nullDoubleToNull(this.sensorLongitude);
    }

    public double getTimeAtServer() {
        return this.timeAtServer;
    }

    public String getRawMessage() {
        return this.rawMessage;
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

    @Override
    public String toCsv() {
        return super.joinCsvColumns(getSensorLatitude(), getSensorLongitude(), getTimeAtServer(), getRawMessage());
    }

    public int compareTo(SensorDatum other) {
        if (this.icao.equals(other.icao)) {
            if (this.timeAtServer < other.timeAtServer) return -1;
            if (this.timeAtServer > other.timeAtServer) return +1;
            return 0;
        }
        return this.icao.compareTo(other.icao);
    }

    // SERIALIZABLE

    private void writeObject(ObjectOutputStream stream) throws IOException {
        stream.writeDouble(sensorLatitude);
        stream.writeDouble(sensorLongitude);
        stream.writeDouble(timeAtServer);
        stream.writeObject(rawMessage);
    }

    private void readObject(ObjectInputStream stream) throws IOException, ClassNotFoundException {
        sensorLatitude = stream.readDouble();
        sensorLongitude = stream.readDouble();
        timeAtServer = stream.readDouble();
        rawMessage = (String) stream.readObject();

        init();
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
                (String) record.get("rawMessage")
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
        List<String> tokens = CsvReader.getTokens(csv);

        if (tokens.size() < 4) throw new IllegalArgumentException("CSV line for SensorDatum should consist of 5 columns");

        double sensorLat = tokens.get(0).length() == 0 ? NULL_DOUBLE : Double.parseDouble(tokens.get(0));
        double sensorLon = tokens.get(1).length() == 0 ? NULL_DOUBLE : Double.parseDouble(tokens.get(1));
        double timeAtServer = Double.parseDouble(tokens.get(2));
        String rawMessage = tokens.get(3);

        return new SensorDatum(sensorLat, sensorLon, timeAtServer, rawMessage);
    }
}
