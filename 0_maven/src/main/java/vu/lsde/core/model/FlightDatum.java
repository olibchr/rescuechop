package vu.lsde.core.model;

import org.opensky.libadsb.Position;
import org.opensky.libadsb.exceptions.MissingInformationException;
import org.opensky.libadsb.msgs.AirspeedHeadingMsg;
import org.opensky.libadsb.msgs.VelocityOverGroundMsg;

import java.util.ArrayList;
import java.util.List;

public class FlightDatum {
    private final static double NULL_DOUBLE = Double.MIN_VALUE;

    private String icao;
    private double time;
    private double longitude;
    private double latitude;
    private double altitude;
    private double heading;
    private double velocity;
    private double rateOfClimb;

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

    // HELP METHODS

    private double nullToNullDouble(Double value) {
        return value == null ? NULL_DOUBLE : value;
    }

    private Double nullDoubleToNull(double value) {
        return value == NULL_DOUBLE ? null : value;
    }

    public static List<FlightDatum> mergeFlightData(Iterable<FlightDatum> flightData) {
        List<FlightDatum> result = new ArrayList<FlightDatum>();

        String icao = null;
        double time = NULL_DOUBLE;
        List<Double> longitudes = new ArrayList<Double>();
        List<Double> latitudes = new ArrayList<Double>();
        List<Double> altitudes = new ArrayList<Double>();
        List<Double> headings = new ArrayList<Double>();
        List<Double> velocities = new ArrayList<Double>();
        List<Double> rocs = new ArrayList<Double>();

        for (FlightDatum fd : flightData) {
            if (time != NULL_DOUBLE && time != fd.getTime() || icao != null && !icao.equals(fd.getIcao())) {
                result.add(new FlightDatum(
                        icao,
                        time,
                        average(longitudes),
                        average(latitudes),
                        average(altitudes),
                        average(headings),
                        average(velocities),
                        average(rocs)));

                longitudes.clear();
                latitudes.clear();
                altitudes.clear();
                headings.clear();
                velocities.clear();
                rocs.clear();
            }

            icao = fd.getIcao();
            time = fd.getTime();
            addIfNotNull(fd.getLongitude(), longitudes);
            addIfNotNull(fd.getLatitude(), latitudes);
            addIfNotNull(fd.getAltitude(), altitudes);
            addIfNotNull(fd.getHeading(), headings);
            addIfNotNull(fd.getVelocity(), velocities);
            addIfNotNull(fd.getRateOfClimb(), rocs);
        }

        return result;
    }

    private static void addIfNotNull(Object value, List list) {
        if (value != null) {
            list.add(value);
        }
    }

    private static Double average(List<Double> doubles) {
        return doubles.isEmpty() ? null : sum(doubles) / doubles.size();
    }

    private static Double sum(List<Double> doubles) {
        double sum = 0;
        for (double d : doubles) {
            sum += d;
        }
        return sum;
    }
}
