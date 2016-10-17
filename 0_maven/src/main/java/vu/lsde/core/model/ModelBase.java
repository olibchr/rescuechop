package vu.lsde.core.model;

import com.google.common.base.Joiner;

import java.io.Serializable;

public abstract class ModelBase implements Serializable {
    private static final Joiner columnJoiner = Joiner.on(",").useForNull("");
    private static final Joiner rowJoiner = Joiner.on("\n").skipNulls();

    protected static final double NULL_DOUBLE = Double.MIN_VALUE;

    public abstract String getIcao();

    public abstract String toCsv();

    protected final String joinCsvColumns(Object first, Object second, Object... rest) {
        return columnJoiner.join(first, second, rest);
    }

    protected final String joinCsvRows(Iterable<String> rows) {
        return rowJoiner.join(rows);
    }

    protected final double nullToNullDouble(Double value) {
        return value == null ? NULL_DOUBLE : value;
    }

    protected final Double nullDoubleToNull(double value) {
        return value == NULL_DOUBLE ? null : value;
    }
}
