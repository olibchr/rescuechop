package vu.lsde.core.util;

import vu.lsde.core.model.FlightDatum;

import java.util.ArrayList;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;


public class Grouping {
    public static SortedMap<Long, List<FlightDatum>> groupFlightDataByMinute(Iterable<FlightDatum> flightData) {
        SortedMap<Long, List<FlightDatum>> flightDataPerMinute = new TreeMap<Long, List<FlightDatum>>();
        for (FlightDatum fd : flightData) {
            long minute = (long) (fd.getTime() / 60);
            List<FlightDatum> list = flightDataPerMinute.get(minute);
            if (list == null) {
                list = new ArrayList<FlightDatum>();
                flightDataPerMinute.put(minute, list);
            }
            list.add(fd);
        }
        return flightDataPerMinute;
    }
}
