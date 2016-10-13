package vu.lsde.core.model;

import org.opensky.libadsb.Position;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

public class AircraftPosition extends ModelBase {
    private static final DateFormat DATE_TIME_FORMAT = new SimpleDateFormat("YYYY-MM-dd HH:mm:ss");

    private final String icao;
    private final double time;
    private final double latitude;
    private final double longitude;
    private final double altitude;

    public AircraftPosition(String icao, double time, double latitude, double longitude, Double altitude) {
        this.icao = icao;
        this.time = time;
        this.latitude = latitude;
        this.longitude = longitude;
        this.altitude = altitude != null ? altitude : Double.MIN_VALUE;
    }

    public AircraftPosition(String icao, double time, Position position) {
        this(icao, time, position.getLatitude(), position.getLongitude(), position.getAltitude());
    }

    // GETTERS

    /**
     * The icao identifying the aircraft.
     *
     * @return
     */
    public String getIcao() {
        return this.icao;
    }

    /**
     * The time at the server when this position was decoded.
     *
     * @return
     */
    public double getTime() {
        return this.time;
    }

    /**
     * The latitude position of the aircraft, in degrees.
     *
     * @return
     */
    public double getLatitude() {
        return this.latitude;
    }

    /**
     * The longitude position of the aircraft, in degrees.
     *
     * @return
     */
    public double getLongitude() {
        return this.longitude;
    }

    /**
     * The altitude of the aircraft, in meters. May be null if altitude is unknown.
     *
     * @return
     */
    public Double getAltitude() {
        return this.altitude > Double.MIN_VALUE ? this.altitude : null;
    }

    /**
     * Whether the altitude of the aircraft is known.
     *
     * @return
     */
    public boolean hasAltitude() {
        return this.altitude > Double.MIN_VALUE;
    }

    // FUNCTIONS

    @Override
    public String toCsv() {
        return this.toCSV(true);
    }

    public String toCSV(boolean prettyTime) {
        Object time = getTime();
        if (prettyTime) {
            Date date = new Date();
            date.setTime((long) getTime() * 1000);
            time = DATE_TIME_FORMAT.format(date);
        }
        return super.joinCsvColumns(getIcao(), time, getLatitude(), getLongitude(), getAltitude());
    }
}
