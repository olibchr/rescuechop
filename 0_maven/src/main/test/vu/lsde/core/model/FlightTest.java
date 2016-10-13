package vu.lsde.core.model;

import junit.framework.TestCase;

import java.util.ArrayList;
import java.util.List;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Created by richa on 12/10/2016.
 */
public class FlightTest extends TestCase {

    public void testGetStartTime() throws Exception {
        // Prepare
        List<FlightDatum> flightData = new ArrayList<FlightDatum>();
        FlightDatum fd = mock(FlightDatum.class);
        when(fd.getTime()).thenReturn(1d);
        flightData.add(fd);
        flightData.add(mock(FlightDatum.class));
        flightData.add(mock(FlightDatum.class));
        Flight flight = new Flight("1", flightData);

        // Act
        double startTime = flight.getStartTime();

        // Assert
        assertEquals(1d, startTime);
    }

    public void testGetEndTime() throws Exception {
        // Prepare
        List<FlightDatum> flightData = new ArrayList<FlightDatum>();
        FlightDatum fd = mock(FlightDatum.class);
        when(fd.getTime()).thenReturn(5d);
        flightData.add(mock(FlightDatum.class));
        flightData.add(mock(FlightDatum.class));
        flightData.add(fd);
        Flight flight = new Flight("1", flightData);

        // Act
        double endTime = flight.getEndTime();

        // Assert
        assertEquals(5d, endTime);
    }

    public void testGetDuration() throws Exception {
        // Prepare
        List<FlightDatum> flightData = new ArrayList<FlightDatum>();
        FlightDatum fd1 = mock(FlightDatum.class);
        when(fd1.getTime()).thenReturn(1d);
        FlightDatum fd2 = mock(FlightDatum.class);
        when(fd2.getTime()).thenReturn(5d);
        flightData.add(fd1);
        flightData.add(mock(FlightDatum.class));
        flightData.add(mock(FlightDatum.class));
        flightData.add(fd2);
        Flight flight = new Flight("1", flightData);

        // Act
        double duration = flight.getDuration();

        // Assert
        assertEquals(4d, duration);
    }

}