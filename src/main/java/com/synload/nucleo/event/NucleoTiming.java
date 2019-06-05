package com.synload.nucleo.event;

public class NucleoTiming {
    private String step;
    private long start;
    private long end;
    private long total;

    public NucleoTiming(String step, long start) {
        this.step = step;
        this.start = start;
    }

    public long getStart() {
        return start;
    }

    public void setStart(long start) {
        this.start = start;
    }

    public long getEnd() {
        return end;
    }

    public void setEnd(long end) {
        this.end = end;
        this.total = end - start;
    }

    public long getTotal() {
        return total;
    }

    public void setTotal(long total) {
        this.total = total;
    }
}
