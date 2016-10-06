package vu.lsde.core.model;

import com.google.common.base.Joiner;

import java.io.Serializable;

public abstract class ModelBase implements Serializable {
    private static final Joiner csvJoiner = Joiner.on(",").useForNull("");

    protected String toCSV(Object first, Object second, Object... rest) {
        return csvJoiner.join(first, second, rest);
    }
}
