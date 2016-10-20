package vu.lsde.core.services;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.opensky.libadsb.Position;
import vu.lsde.core.io.CsvReader;

import java.util.List;


public abstract class PositionService {
    protected final static List<Position> getPositionsFromHdfs(JavaSparkContext sc, String inputPath) {
        JavaRDD<String> lines = sc.textFile(inputPath);
        JavaRDD<Position> positions = lines.map(new Function<String, Position>() {
            @Override
            public Position call(String s) throws Exception {
                List<String> tokens = CsvReader.getTokens(s);
                double lat = Double.parseDouble(tokens.get(0));
                double lon = Double.parseDouble(tokens.get(1));
                return new Position(lon, lat, 0.0);
            }
        });
        return positions.collect();
    }
}
