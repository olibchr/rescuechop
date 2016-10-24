package vu.lsde.core.util;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by Richard on 11-10-2016.
 */
public class Util {

    public static <T> List<T> addList(List<T> list1, List<T> list2) {
        List<T> result = new ArrayList<T>();

        if (list1 != null) {
            for (T item : list1) {
                result.add(item);
            }
        }
        if (list2 != null) {
            for (T item : list2) {
                result.add(item);
            }
        }

        return result;
    }

    /**
     * Return the first argument that is not null, or null if all arguments are null.
     *
     * @param first
     * @param second
     * @param others
     * @param <T>
     * @return
     */
    public static <T> T firstNonNull(T first, T second, T... others) {
        if (first != null) {
            return first;
        } else if (second != null) {
            return second;
        } else {
            for (T other : others) {
                if (other != null) {
                    return other;
                }
            }
        }
        return null;
    }

    /**
     * Add value to list if the value is not null.
     *
     * @param list
     * @param value
     */
    public static void addIfNotNull(List list, Object value) {
        if (value != null) {
            list.add(value);
        }
    }

    /**
     * Return the average value of the provided values.
     *
     * @param values
     * @return
     */
    public static Double avg(List<Double> values) {
        if (values.isEmpty())
            return null;
        if (values.size() == 1)
            return values.get(0);

        double sum = 0;
        for (double value : values) {
            sum += value;
        }
        return sum / values.size();
    }

    /**
     * Return the average of the provided angles in degrees.
     *
     * @param values
     * @return
     */
    public static Double avgAngle(List<Double> values) {
        if (values.isEmpty())
            return null;
        if (values.size() == 1)
            return values.get(0);

        double sumSin = 0;
        double sumCos = 0;
        for (double value : values) {
            sumSin += Math.sin(Math.toRadians(value));
            sumCos += Math.cos(Math.toRadians(value));
        }
        return Math.toDegrees(Math.atan2(sumSin, sumCos));
    }

    public static double angleDistance(double a1, double a2) {
        double phi = Math.abs(a2 - a1) % 360;
        return phi > 180 ? 360 - phi : phi;
    }
}
