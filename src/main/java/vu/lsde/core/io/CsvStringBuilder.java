package vu.lsde.core.io;

public class CsvStringBuilder {
    private StringBuilder sb;

    public CsvStringBuilder addValue(Object value) {
        if (this.sb == null) {
            this.sb = new StringBuilder();
        } else {
            this.sb.append(",");
        }

        if (value != null) {
            this.sb.append(value.toString());
        }

        return this;
    }

    public String toString() {
        return sb.toString();
    }
}
