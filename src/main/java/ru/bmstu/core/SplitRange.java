package ru.bmstu.core;

public class SplitRange {
    private long from;
    private long to;

    private SplitRange(long from, long to) {
        this.from = from;
        this.to = to;
    }

    static SplitRange of(long from, long to) {
        return new SplitRange(from, to);
    }

    public long getFrom() {
        return from;
    }

    public long getTo() {
        return to;
    }
}
