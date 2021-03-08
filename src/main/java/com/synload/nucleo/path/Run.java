package com.synload.nucleo.path;

import java.util.LinkedList;
import java.util.List;

public class Run {
    List<Run> nextRuns = new LinkedList<>();
    boolean parallel = false;
    public List<Run> getNextRuns() {
        return nextRuns;
    }

    public void setNextRuns(List<Run> nextRuns) {
        this.nextRuns = nextRuns;
    }

    public boolean isParallel() {
        return parallel;
    }

    public void setParallel(boolean parallel) {
        this.parallel = parallel;
    }
}
