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
}
