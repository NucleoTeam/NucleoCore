package com.synload.nucleo.event;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class NucleoChain implements Serializable {
    public String[] chainString = null;
    public List<NucleoChain> parallelChains = new ArrayList<>();
    public int part = 0;
    public int parallel = -1;
    public boolean recombined = false;
    public boolean complete = false;
    public int stepStart = -1;

    public NucleoChain(NucleoChain current) {
        this.chainString = current.getChainString();
        for(NucleoChain nc : current.parallelChains){
            parallelChains.add(new NucleoChain(nc));
        }
        this.part = current.getPart();
        this.parallel = current.getParallel();
        this.complete = current.isComplete();
        this.recombined = current.isRecombined();
        this.stepStart = current.stepStart;
    }

    public NucleoChain() {
    }

    public String[] getChainString() {
        return chainString;
    }

    public void setChainString(String[] chainString) {
        this.chainString = chainString;
    }

    public List<NucleoChain> getParallelChains() {
        return parallelChains;
    }

    public void setParallelChains(List<NucleoChain> parallelChains) {
        this.parallelChains = parallelChains;
    }

    public int getPart() {
        return part;
    }

    public void setPart(int part) {
        this.part = part;
    }

    public int getParallel() {
        return parallel;
    }

    public void setParallel(int parallel) {
        this.parallel = parallel;
    }

    public boolean isRecombined() {
        return recombined;
    }

    public void setRecombined(boolean recombined) {
        this.recombined = recombined;
    }

    public boolean isComplete() {
        return complete;
    }

    public void setComplete(boolean complete) {
        this.complete = complete;
    }

    public int getStepStart() {
        return stepStart;
    }

    public void setStepStart(int stepStart) {
        this.stepStart = stepStart;
    }

}
