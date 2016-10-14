package vu.lsde.core.model;

import java.util.ArrayList;
import java.util.List;
import java.util.SortedSet;
import java.util.UUID;

public class Flight extends ModelBase {
    private final String id;
    private final String icao;
    private final SortedSet<FlightDatum> flightData;

    public Flight(String icao, SortedSet<FlightDatum> flightData) {
        this.id = UUID.randomUUID().toString();
        this.icao = icao;
        this.flightData = flightData;
    }

    public String getID() {
        return this.id;
    }

    public String getIcao() {
        return this.icao;
    }

    public SortedSet<FlightDatum> getFlightData() {
        return this.flightData;
    }

    public double getStartTime() {
        return flightData.first().getTime();
    }

    public double getEndTime() {
        return flightData.last().getTime();
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
