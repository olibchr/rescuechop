package vu.lsde.core.services;

import org.apache.spark.api.java.JavaSparkContext;
import org.opensky.libadsb.Position;

import java.util.List;

public class HospitalPositionsService extends PositionService {

    public static List<Position> getHospitalPositions(JavaSparkContext sc) {
        return getPositionsFromHdfs(sc, "europe_hospitals.csv");
    }
}
