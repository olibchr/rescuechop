package vu.lsde.core.model;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class Flight extends ModelBase {
    private final String id;
    private final String icao;
    private final List<FlightDatum> flightData;

    public Flight(String icao, List<FlightDatum> flightData) {
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

    public List<FlightDatum> getFlightData() {
        return this.flightData;
    }

    public double getStartTime() {
        return flightData.get(0).getTime();
    }

    public double getEndTime() {
        return flightData.get(flightData.size() - 1).getTime();
    }

    public double getDuration() {
        if (flightData.isEmpty())
            return 0;
        else
            return getEndTime() - getStartTime();
    }

    public List<String> toCSV() {
        return this.toCSV(false);
    }

    public List<String> toCSV(boolean prettyTime) {
        List<String> lines = new ArrayList<String>();
        for (FlightDatum fd : flightData) {
            lines.add(toCSV(getID(), fd.toCSV(prettyTime)));
        }
        return lines;
    }


}
