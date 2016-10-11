package vu.lsde.core.model;

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

    public String toCSV() {
        String csv = null;
        if (!flightData.isEmpty()) {
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < flightData.size() - 1; i++) {
                sb.append(flightData.get(i).toCSV()).append('\n');
            }
            sb.append(flightData.get(flightData.size() - 1).toCSV());
            csv = sb.toString();
        }
        return csv;
    }
}
