package vu.lsde.core.model;

import junit.framework.TestCase;
import org.opensky.libadsb.Position;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.mockito.Mockito.*;

public class FlightDatumTest extends TestCase {

    public void testCompareTo() {
        // Prepare
        Position pos = new Position(0d, 0d, 0d);
        FlightDatum fd1 = new FlightDatum("1", 1, pos);
        FlightDatum fd2 = new FlightDatum("1", 2, pos);
        FlightDatum fd3 = new FlightDatum("1", 4, pos);
        FlightDatum fd4 = new FlightDatum("2", 1, pos);
        FlightDatum fd5 = new FlightDatum("2", 2, pos);
        List<FlightDatum> flightData = new ArrayList<FlightDatum>();
        flightData.add(fd3);
        flightData.add(fd1);
        flightData.add(fd5);
        flightData.add(fd2);
        flightData.add(fd4);

        // Act
        Collections.sort(flightData);

        // Assert
        assertEquals(flightData.indexOf(fd1), 0);
        assertEquals(flightData.indexOf(fd2), 1);
        assertEquals(flightData.indexOf(fd3), 2);
        assertEquals(flightData.indexOf(fd4), 3);
        assertEquals(flightData.indexOf(fd5), 4);
    }

    public void testExtend() {
        // Prepare
        FlightDatum base = new FlightDatum("icao", 1, 0.0, 0.0, null, null, null, null);
        FlightDatum other = new FlightDatum("icao2", 2, 10.0, 10.0, 200.0, 45.0, 100.0, 5.0);

        // Act
        FlightDatum extended = base.extend(other);

        // Assert
        assertEquals(extended.getIcao(), "icao");
        assertEquals(extended.getLongitude(), 0.0);
        assertEquals(extended.getLatitude(), 0.0);
        assertEquals(extended.getAltitude(), 200.0);
        assertEquals(extended.getHeading(), 45.0);
        assertEquals(extended.getVelocity(), 100.0);
        assertEquals(extended.getRateOfClimb(), 5.0);
    }

    public void testExtendProvideNull() {
        // Prepare
        FlightDatum flightDatum = new FlightDatum("icao", 1, 0.0, 0.0, null, null, null, null);

        // Act
        FlightDatum extended = flightDatum.extend(null);

        // Assert
        assertSame(flightDatum, extended);
    }

}