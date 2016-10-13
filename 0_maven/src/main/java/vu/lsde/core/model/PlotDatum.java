package vu.lsde.core.model;

import vu.lsde.core.reducer.Point;

import java.util.ArrayList;
import java.util.List;

public class PlotDatum extends ModelBase implements Point {
    private String icao;
    private String flightID;
    private double time;
    private double latitude;
    private double longitude;
    private double altitude;

    private PlotDatum(String flightID, FlightDatum flightDatum) {
        this.icao = flightDatum.getIcao();
        this.flightID = flightID;
        this.time = flightDatum.getTime();
        this.latitude = flightDatum.getLatitude();
        this.longitude = flightDatum.getLongitude();
        this.altitude = nullToNullDouble(flightDatum.getAltitude());
    }

    // GETTERS

    public String getIcao() {
        return icao;
    }

    public String getFlightID() {
        return flightID;
    }

    public double getTime() {
        return time;
    }

    public double getLatitude() {
        return latitude;
    }

    public double getLongitude() {
        return longitude;
    }

    public Double getAltitude() {
        return nullDoubleToNull(altitude);
    }

    public double getX() {
        return getLongitude() * 100000;
    }

    public double getY() {
        return getLatitude() * 100000;
    }

    // CSV

    @Override
    public String toCsv() {
        return super.joinCsvColumns(getFlightID(), getIcao(), getTime(), getLatitude(), getLongitude(), getAltitude());
    }

    // STATIC

    public static List<PlotDatum> fromFlight(Flight flight) {
        List<PlotDatum> result = new ArrayList<PlotDatum>();
        for (FlightDatum fd : flight.getFlightData()) {
            if (fd.hasPosition()) {
                result.add(new PlotDatum(flight.getID(), fd));
            }
        }
        return result;
    }
}