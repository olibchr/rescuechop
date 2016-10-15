package vu.lsde.core.util;

import vu.lsde.core.model.FlightDatum;

import java.util.ArrayList;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;


public class Grouping {

    public static SortedMap<Long, List<FlightDatum>> groupFlightDataByTimeWindow(Iterable<FlightDatum> flightData, double windowLength) {
        SortedMap<Long, List<FlightDatum>> flightDataPerMinute = new TreeMap<>();
        for (FlightDatum fd : flightData) {
            long window = (long) Math.floor(fd.getTime() / windowLength);
            List<FlightDatum> list = flightDataPerMinute.get(window);
            if (list == null) {
                list = new ArrayList<>();
                flightDataPerMinute.put(window, list);
            }
            list.add(fd);
        }
        return flightDataPerMinute;
    }

}
