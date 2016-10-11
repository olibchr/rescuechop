package vu.lsde.core.io;

import java.util.ArrayList;
import java.util.List;

public class CsvReader {
    public static List<String> getTokens(String line) {
        List<String> tokens = new ArrayList<String>();

        int start = 0;
        int end = 0;
        while (end < line.length()) {
            if (line.charAt(end) == ',') {
                tokens.add(line.substring(start, end));
                start = end + 1;
            }
            end++;
        }
        tokens.add(line.substring(start, end));

        return tokens;
    }
}
