package vu.lsde.core.model;

import org.opensky.libadsb.Position;
import org.opensky.libadsb.exceptions.MissingInformationException;
import org.opensky.libadsb.msgs.AirspeedHeadingMsg;
import org.opensky.libadsb.msgs.VelocityOverGroundMsg;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class FlightDatum extends ModelBase {
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

    // INSTANCE METHODS

    public String toCSV() {
        return toCSV(false);
    }

    public String toCSV(boolean prettyTime) {
        Object time = getTime();
        if (prettyTime) {
            Date date = new Date();
            date.setTime((long) getTime() * 1000);
            time = DATE_TIME_FORMAT.format(date);
        }
        return super.toCSV(getIcao(), time, getLatitude(), getLongitude(), getAltitude(), getHeading(), getVelocity(), getRateOfClimb());
    }

    // HELP METHODS

    private double nullToNullDouble(Double value) {
        return value == null ? NULL_DOUBLE : value;
    }

    private Double nullDoubleToNull(double value) {
        return value == NULL_DOUBLE ? null : value;
    }

    // STATIC METHODS

    /**
     * Merges given flight data into one flight datum object. All the flight data should belong to the same aircraft
     * and timestamp.
     *
     * @param icao
     * @param time
     * @param flightData
     * @return
     */
    public static FlightDatum mergeFlightData(String icao, double time, Iterable<FlightDatum> flightData) {
        List<Double> lons = new ArrayList<Double>();
        List<Double> lats = new ArrayList<Double>();
        List<Double> alts = new ArrayList<Double>();
        List<Double> headings = new ArrayList<Double>();
        List<Double> velos = new ArrayList<Double>();
        List<Double> rocs = new ArrayList<Double>();

        for (FlightDatum fd : flightData) {
            if (time != fd.getTime() || !icao.equals(fd.getIcao())) {
                throw new IllegalArgumentException("FlightDatum does not have correct time or icao");
            }
            addIfNotNull(fd.getLongitude(), lons);
            addIfNotNull(fd.getLatitude(), lats);
            addIfNotNull(fd.getAltitude(), alts);
            addIfNotNull(fd.getHeading(), headings);
            addIfNotNull(fd.getVelocity(), velos);
            addIfNotNull(fd.getRateOfClimb(), rocs);
        }

        return new FlightDatum(icao, time, avg(lons), avg(lats), avg(alts), avg(headings), avg(velos), avg(rocs));
    }

    /**
     * Add a value to a list only if the value is not null.
     *
     * @param value
     * @param list
     */
    private static void addIfNotNull(Object value, List list) {
        if (value != null) {
            list.add(value);
        }
    }

    /**
     * Return the average of the provided values, or null if no values were provided.
     *
     * @param doubles
     * @return
     */
    private static Double avg(Iterable<Double> doubles) {
        double sum = 0, n = 0;
        for (double d : doubles) {
            sum += d; n++;
        }
        return n > 0 ? sum / n : null;
    }
}
