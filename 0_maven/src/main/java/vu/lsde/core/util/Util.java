package vu.lsde.core.util;

/**
 * Created by Richard on 11-10-2016.
 */
public class Util {

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
