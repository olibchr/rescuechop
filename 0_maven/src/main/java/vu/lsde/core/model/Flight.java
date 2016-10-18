package vu.lsde.core.model;

import javafx.geometry.Pos;
import org.opensky.libadsb.Position;

import java.util.*;

public class Flight extends ModelBase {
    private String id;
    private String icao;
    private SortedSet<FlightDatum> flightData;
    private double startLatitude;
    private double startLongitude;
    private double startAltitude;
    private double endLatitude;
    private double endLongitude;
    private double endAltitude;

    public Flight(String icao, SortedSet<FlightDatum> flightData) {
        this.id = UUID.randomUUID().toString();
        this.icao = icao;
        this.flightData = flightData;

        init();
    }

    private void init() {
        Position startPosition = null;
        Double startAltitude = null;
        Position endPosition = null;
        Double endAltitude = null;
        for (FlightDatum fd : flightData) {
            if (fd.hasPosition()) {
                Position position = fd.getPosition();
                if (startPosition == null) {
                    startPosition = position;
                }
                endPosition = position;
            }
            if (fd.hasAltitude()) {
                double altitude = fd.getAltitude();
                if (startAltitude == null) {
                    startAltitude = altitude;
                }
                endAltitude = altitude;
            }
        }
        if (startPosition != null) {
            this.startLatitude = startPosition.getLatitude();
            this.startLongitude = startPosition.getLongitude();
            this.endLatitude = endPosition.getLatitude();
            this.endLongitude = endPosition.getLongitude();
        }
        this.startAltitude = nullToNullDouble(startAltitude);
        this.endLatitude = nullToNullDouble(endAltitude);
    }

    public String getID() {
        return this.id;
    }

    @Override
    public String getIcao() {
        return this.icao;
    }

    public SortedSet<FlightDatum> getFlightData() {
        return Collections.unmodifiableSortedSet(this.flightData);
    }

    public double getStartTime() {
        return flightData.first().getTime();
    }

    public double getEndTime() {
        return flightData.last().getTime();
    }

    public Position getStartPosition() {
        return new Position(startLongitude, startLatitude, nullDoubleToNull(startAltitude));
    }

    public Position getEndPosition() {
        return new Position(endLongitude, endLatitude, nullDoubleToNull(endAltitude));
    }

    public double getDuration() {
        if (flightData.isEmpty())
            return 0;
        else
            return getEndTime() - getStartTime();
    }

    @Override
    public String toCsv() {
        return toCSV(false);
    }

    public String toCSV(boolean prettyTime) {
        List<String> lines = new ArrayList<>();
        for (FlightDatum fd : flightData) {
            lines.add(joinCsvColumns(getID(), fd.toCSV(prettyTime)));
        }
        return joinCsvRows(lines);
    }
}
