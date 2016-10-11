package vu.lsde.jobs;

import junit.framework.TestCase;
import org.opensky.libadsb.Position;
import vu.lsde.core.model.FlightDatum;

import java.util.ArrayList;
import java.util.List;

public class MergeFlightDataTest extends TestCase {
    public void testMergeFlightData() throws Exception {
        // Prepare
        List<FlightDatum> flightData = new ArrayList<FlightDatum>();
        Position pos = new Position(0d, 0d, 0d);
        flightData.add(new FlightDatum("1", 1.0, pos));
        flightData.add(new FlightDatum("1", 2.0, pos));
        flightData.add(new FlightDatum("1", 1.1, pos));
        flightData.add(new FlightDatum("1", 2.0, pos));
        flightData.add(new FlightDatum("1", 2.2, pos));

        // Act
        List<FlightDatum> mergedFlightData = MergeFlightData.mergeFlightData(flightData);

        // Assert
        assertEquals(5, flightData.size());
        assertEquals(2, mergedFlightData.size());
        assertEquals(1d, mergedFlightData.get(0).getTime());
        assertEquals(2d, mergedFlightData.get(1).getTime());
    }

}