package vu.lsde.core.services;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.opensky.libadsb.Position;
import vu.lsde.core.io.CsvReader;

import java.util.List;

public class HelipadPositionsService extends PositionService {

    public static List<Position> getHelipadPositions(JavaSparkContext sc) {
        return getPositionsFromHdfs(sc, "europe_helipads.csv");
    }
}
