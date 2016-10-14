package vu.lsde.core.model;

import org.opensky.libadsb.Position;
import org.opensky.libadsb.exceptions.MissingInformationException;
import org.opensky.libadsb.msgs.AirspeedHeadingMsg;
import org.opensky.libadsb.msgs.VelocityOverGroundMsg;
import vu.lsde.core.io.CsvReader;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

import static vu.lsde.core.util.Util.firstNonNull;

public class FlightDatum extends ModelBase implements Comparable<FlightDatum> {
    private static final DateFormat DATE_TIME_FORMAT = new SimpleDateFormat("YYYY-MM-dd HH:mm:ss");
    private final static double NULL_DOUBLE = Double.MIN_VALUE;

    private String icao;
    private double time;
    private double longitude;
    private double latitude;
    private double altitude;
    private double heading;
    private double velocity;
    private double rateOfClimb;

    // CONSTRUCTORS

    public FlightDatum(String icao, double time, Double lon, Double lat, Double alt, Double heading, Double velo, Double roc) {
        init(icao, time, lon, lat, alt, heading, velo, roc);
    }

    public FlightDatum(String icao, double time, Position pos) {
        init(icao, time, pos.getLongitude(), pos.getLatitude(), pos.getAltitude(), null, null, null);
    }

    public FlightDatum(String icao, double time, AirspeedHeadingMsg msg) {
        try {
            Double heading = msg.hasHeadingInfo() ? msg.getHeading() : null;
            Double velocity = msg.hasAirspeedInfo() ? msg.getAirspeed() : null;
            Double roc = msg.hasVerticalRateInfo() ? msg.getVerticalRate() : null;

            init(icao, time, null, null, null, heading, velocity, roc);
        } catch (MissingInformationException e) {
            throw new RuntimeException(e);
        }
    }

    public FlightDatum(String icao, double time, VelocityOverGroundMsg msg) {
        try {
            Double heading = msg.hasVelocityInfo() ? msg.getHeading() : null;
            Double velocity = msg.hasVelocityInfo() ? msg.getVelocity() : null;
            Double roc = msg.hasVerticalRateInfo() ? msg.getVerticalRate() : null;

            init(icao, time, null, null, null, heading, velocity, roc);
        } catch (MissingInformationException e) {
            throw new RuntimeException(e);
        }
    }

    private void init(String icao, double time, Double lon, Double lat, Double alt, Double heading, Double velo, Double roc) {
        this.icao = icao;
        this.time = time;
        this.longitude = nullToNullDouble(lon);
        this.latitude = nullToNullDouble(lat);
        this.altitude = nullToNullDouble(alt);
        this.heading = nullToNullDouble(heading);
        this.velocity = nullToNullDouble(velo);
        this.rateOfClimb = nullToNullDouble(roc);
    }

    // GETTERS

    public String getIcao() {
        return icao;
    }

    public double getTime() {
        return time;
    }

    public Double getLongitude() {
        return nullDoubleToNull(longitude);
    }

    public Double getLatitude() {
        return nullDoubleToNull(latitude);
    }

    public Double getAltitude() {
        return nullDoubleToNull(altitude);
    }

    public Double getHeading() {
        return nullDoubleToNull(heading);
    }

    public Double getVelocity() {
        return nullDoubleToNull(velocity);
    }

    public Double getRateOfClimb() {
        return nullDoubleToNull(rateOfClimb);
    }

    public Position getPosition() {
        if (getLatitude() == null || getLongitude() == null)
            return null;
        else
            return new Position(getLongitude(), getLatitude(), getAltitude());
    }

    public boolean hasPosition() {
        return getLatitude() != null && getLongitude() != null;
    }

    public boolean hasAltitude() {
        return getAltitude() != null;
    }

    public boolean hasHeading() {
        return getHeading() != null;
    }

    public boolean hasVelocity() {
        return getVelocity() != null;
    }

    public boolean hasRateOfClimb() {
        return getRateOfClimb() != null;
    }

    // INSTANCE METHODS

    public FlightDatum extend(FlightDatum other) {
        if (other == null) {
            return this;
        } else {
            return new FlightDatum(icao, time,
                    firstNonNull(this.getLongitude(), other.getLongitude()),
                    firstNonNull(this.getLatitude(), other.getLatitude()),
                    firstNonNull(this.getAltitude(), other.getAltitude()),
                    firstNonNull(this.getHeading(), other.getHeading()),
                    firstNonNull(this.getVelocity(), other.getVelocity()),
                    firstNonNull(this.getRateOfClimb(), other.getRateOfClimb()));
        }
    }

    @Override
    public boolean equals(Object other) {
        if (other instanceof FlightDatum) {
            FlightDatum o = (FlightDatum) other;
            return this.icao.equals(o.icao)
                    && this.time == o.time
                    && this.longitude == o.longitude
                    && this.latitude == o.latitude
                    && this.altitude == o.altitude
                    && this.heading == o.heading
                    && this.velocity == o.velocity
                    && this.rateOfClimb == o.rateOfClimb;
        }
        return false;
    }

    public int compareTo(FlightDatum other) {
        int result = this.icao.compareTo(other.icao);
        if (result != 0)
            return result;
        result = Double.compare(this.time, other.time);
        if (result != 0)
            return result;
        // any comparing from this point is just to make compareTo consistent with equals
        result = Double.compare(this.longitude, other.longitude);
        if (result != 0)
            return result;
        result = Double.compare(this.latitude, other.latitude);
        if (result != 0)
            return result;
        result = Double.compare(this.altitude, other.altitude);
        if (result != 0)
            return result;
        result = Double.compare(this.heading, other.heading);
        if (result != 0)
            return result;
        result = Double.compare(this.velocity, other.velocity);
        if (result != 0)
            return result;
        return Double.compare(this.rateOfClimb, other.rateOfClimb);
    }

    @Override
    public String toCsv() {
        return toCSV(false);
    }

    public String toCSV(boolean prettyTime) {
        Object time = getTime();
        if (prettyTime) {
            Date date = new Date();

            date.setTime(((long) getTime()) * 1000);
            time = DATE_TIME_FORMAT.format(date);
        }
        return super.joinCsvColumns(getIcao(), time, getLatitude(), getLongitude(), getAltitude(), getHeading(), getVelocity(), getRateOfClimb());
    }

    // STATIC METHODS

    public static FlightDatum fromCSV(String csv) {
        List<String> tokens = CsvReader.getTokens(csv);

        if (tokens.size() < 8) throw new IllegalArgumentException("CSV line for FlightDatum should consist of 8 columns");

        String icao = tokens.get(0);
        double time = Double.parseDouble(tokens.get(1));
        Double lat = doubleOrNull(tokens.get(2));
        Double lon = doubleOrNull(tokens.get(3));
        Double alt = doubleOrNull(tokens.get(4));
        Double heading = doubleOrNull(tokens.get(5));
        Double velo = doubleOrNull(tokens.get(6));
        Double roc = doubleOrNull(tokens.get(7));

        return new FlightDatum(icao, time, lon, lat, alt, heading, velo, roc);
    }

    private static Double doubleOrNull(String s) {
        if (s.length() > 0) {
            return Double.parseDouble(s);
        }
        return null;
    }
}
