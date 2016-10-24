package vu.lsde.core.util;

import junit.framework.TestCase;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by Richard on 24-10-2016.
 */
public class UtilTest extends TestCase {
    public void testAvgAngle() throws Exception {
        // Prepare
        List<Double> angles = new ArrayList<>();
        angles.add(315.0);
        angles.add(45.0);
        angles.add(0.0);

        // Act
        double avgAngle = Util.avgAngle(angles);

        // Assert
        assertEquals(0.0, avgAngle, 0.1);
    }

    public void testAngleDistance() throws Exception {
        // Prepare
        double a1 = 355.0;
        double a2 = 5.0;

        // Act
        double distance = Util.angleDistance(a1, a2);

        // Assert
        assertEquals(10.0, distance);
    }

}