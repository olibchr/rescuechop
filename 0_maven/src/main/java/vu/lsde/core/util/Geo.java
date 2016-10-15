package vu.lsde.core.util;

import org.opensky.libadsb.Position;

import java.util.Collection;
import java.util.List;

/**
 * Created by richa on 15/10/2016.
 */
public class Geo {
    public static Position findCentralPosition(List<Position> positions) {
        if (positions.isEmpty())
            return null;
        if (positions.size() == 1)
            return positions.get(0);

        double x = 0;
        double y = 0;
        double z = 0;

        for (Position position : positions) {
            double latRadians = Math.toRadians(position.getLatitude());
            double lonRadians = Math.toRadians(position.getLongitude());
            double latCos = Math.cos(latRadians);
            double latSin = Math.sin(latRadians);
            double lonCos = Math.cos(lonRadians);
            double lonSin = Math.sin(lonRadians);

            x += latCos * lonCos;
            y += latCos * lonSin;
            z += latSin;
        }

        x /= positions.size();
        y /= positions.size();
        z /= positions.size();

        double centralLon = Math.atan2(y, x);
        double centralSqrt = Math.sqrt(x * x + y * y);
        double centralLat = Math.atan2(z, centralSqrt);

        return new Position(centralLon, centralLat, null);
    }
}
