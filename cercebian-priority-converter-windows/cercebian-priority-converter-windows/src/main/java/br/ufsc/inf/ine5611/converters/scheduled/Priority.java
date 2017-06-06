package br.ufsc.inf.ine5611.converters.scheduled;

import java.util.ArrayList;
import java.util.List;

public enum Priority {
    LOW,
    NORMAL,
    HIGH;

    private static List<Priority> decreasing;
    public static List<Priority> decreasing() {
        if (decreasing == null) {
            decreasing = new ArrayList<>(values().length);
            for (Priority p : values()) decreasing.add(0, p);
        }
        return decreasing;
    }
}
