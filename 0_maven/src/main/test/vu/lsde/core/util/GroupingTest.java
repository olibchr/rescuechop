package vu.lsde.core.util;

import junit.framework.TestCase;
import vu.lsde.core.model.Flight;
import vu.lsde.core.model.FlightDatum;

import java.util.ArrayList;
import java.util.List;
import java.util.SortedMap;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Created by richa on 15/10/2016.
 */
public class GroupingTest extends TestCase {
    public void setUp() throws Exception {
        super.setUp();

    }

    public void tearDown() throws Exception {

    }

    public void testGroupFlightDataByTimeWindow() throws Exception {
        List<FlightDatum> flightData = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            flightData.add(mock(FlightDatum.class));
            when(flightData.get(i).getTime()).thenReturn((double)i);
        }

        SortedMap<Long, List<FlightDatum>> map = Grouping.groupFlightDataByTimeWindow(flightData, 5);

        List<FlightDatum> flightData0 = map.get(0L);
        List<FlightDatum> flightData1 = map.get(1L);
        for (int i = 0; i < 5; i++) {
            assertEquals(flightData.get(i), flightData0.get(i));
        }
        for (int i = 5; i < 10; i++) {
            assertEquals(flightData.get(i), flightData1.get(i - 5));
        }
    }

}