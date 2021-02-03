package com.synload.nucleo.data;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.common.collect.Lists;
import com.synload.nucleo.event.NucleoChain;
import com.synload.nucleo.event.NucleoChainStatus;
import com.synload.nucleo.event.NucleoStep;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.*;

public class NucleoData implements Cloneable, Serializable {
    protected static final Logger logger = LoggerFactory.getLogger(NucleoData.class);
    private UUID root;
    private List<NucleoChain> chainList = new ArrayList<>();
    private String origin;
    private List<NucleoStep> steps = new ArrayList<>();
    private NucleoStep execution = new NucleoStep();
    private int onChain = -1;
    private int track = 0;
    private Stack<Object[]> timeExecutions = new Stack<>();
    private long timeTrack = System.currentTimeMillis();
    private int retries = 0;
    private int version = 0;
    private NucleoObject objects = new NucleoObject();
    private NucleoChainStatus chainBreak = new NucleoChainStatus();

    public NucleoData() {
        root = UUID.randomUUID();
        getExecution().setStart(System.currentTimeMillis());
    }

    public NucleoData(NucleoData data) {
        this.root = data.root;
        data.chainList.stream().forEach(x -> this.getChainList().add(new NucleoChain(x)));
        this.origin = data.origin;
        data.steps.stream().forEach(x -> this.steps.add(new NucleoStep(x)));
        this.execution = new NucleoStep(data.execution);
        this.onChain = data.onChain;
        this.track = data.track;
        if(data.timeExecutions!=null) {
            this.timeExecutions = (Stack<Object[]>) data.timeExecutions.clone();
        }else{
            timeExecutions = new Stack<>();
        }
        this.timeTrack = data.timeTrack;
        this.retries = data.retries;
        this.version = data.version;
        this.objects = new NucleoObject(data.getObjects());
        this.chainBreak = new NucleoChainStatus(data.chainBreak);
    }

    public long markTime() {
        //long total = System.currentTimeMillis() - timeTrack;
        //timeTrack = System.currentTimeMillis();
        //timeExecutions.add(new Object[]{total});
        return 0;
    }

    public long markTime(String message) {
        //long total = System.currentTimeMillis() - timeTrack;
        //timeTrack = System.currentTimeMillis();
        //timeExecutions.add(new Object[]{message, total});
        return 0;
    }

    public void buildChains(String[] chains) {
        for (String chain : chains) {
            NucleoChain nucleoChain = buildChains(chain);
            this.getChainList().add(nucleoChain);
        }
    }

    public NucleoChain buildChains(String chain) {
        if (chain == null || chain.length() == 0)
            return null;
        if (chain.startsWith("[")) {
            String[] parallels = chain.substring(1, chain.length() - 1).split("/");
            NucleoChain chainParallel = new NucleoChain();
            //logger.debug(this.getRoot().toString() + " - " + "Parallel chain");
            for (String parallel : parallels) {
                NucleoChain parallelInner = new NucleoChain();
                parallelInner.setChainString(parallel.split("\\."));
                chainParallel.getParallelChains().add(parallelInner);
            }
            return chainParallel;
        } else {
            NucleoChain singleChain = new NucleoChain();
            singleChain.setChainString(chain.split("\\."));
            //logger.debug(this.getRoot().toString() + " - " + "Single chain request");
            return singleChain;
        }
    }

    public boolean previousParallelStep(){
        if(this.previousParentChain()!=null){
            return !this.previousParentChain().parallelChains.isEmpty();
        }
        return false;
    }

    public boolean onParallelStep(){
        if(this.currentParentChain()!=null){
            return !this.currentParentChain().parallelChains.isEmpty();
        }
        return false;
    }

    public String depthChainString(NucleoChain chain) {
        if (chain != null) {
            return String.join(".", Arrays.copyOfRange(chain.getChainString(), 0, chain.part + 1));
        }
        return null;
    }

    public NucleoChain currentChain() {
        if (this.getChainList().size() <= onChain) {
            return null;
        }
        return depthChain(chainList.get(onChain));
    }
    public NucleoChain currentParentChain() {
        if (this.getChainList().size() <= onChain) {
            return null;
        }
        return chainList.get(onChain);
    }

    public NucleoChain previousChain() {
        if (onChain <= 0) {
            return null;
        }
        return depthChain(chainList.get(onChain - 1));
    }

    public NucleoChain previousParentChain() {
        if (onChain <= 0) {
            return null;
        }
        return chainList.get(onChain - 1);
    }

    public NucleoChain depthChain(NucleoChain chain) {
        if (chain.getChainString() != null) {
            return chain;
        } else if (chain.getParallel() != -1) {
            return depthChain(chain.getParallelChains().get(chain.getParallel()));
        }
        return null;
    }

    public int parallel(int direction) {
        if (this.getOnChain() + direction < 0 || this.getOnChain() + direction > this.getChainList().size())
            return 0;

        return this.getChainList().get(this.getOnChain() + direction).getParallelChains().size();
    }

    public String currentChainString() {
        if (this.getChainList().size() == 0 || this.getChainList().size() <= this.onChain)
            return null;
        return depthChainString(this.currentChain());
    }

    public void setStepsStart() {
        NucleoChain chain = currentChain();
        if (chain.stepStart == -1) {
            chain.setStepStart(steps.size());
        }
    }

    public NucleoObject getObjects() {
        return objects;
    }

    public void setObjects(NucleoObject objects) {
        this.objects = objects;
    }

    public String getOrigin() {
        return origin;
    }

    public void setOrigin(String origin) {
        this.origin = origin;
    }

    public UUID getRoot() {
        return root;
    }

    public void setRoot(UUID root) {
        this.root = root;
    }

    public NucleoChainStatus getChainBreak() {
        return chainBreak;
    }

    public void setChainBreak(NucleoChainStatus chainBreak) {
        this.chainBreak = chainBreak;
    }

    public List<NucleoChain> getChainList() {
        return chainList;
    }

    public void setChainList(List<NucleoChain> chainList) {
        this.chainList = chainList;
    }

    public int getOnChain() {
        return onChain;
    }

    public void setOnChain(int onChain) {
        this.onChain = onChain;
    }

    public List<NucleoStep> getSteps() {
        return steps;
    }

    public void setSteps(List<NucleoStep> steps) {
        this.steps = steps;
    }

    public NucleoStep getExecution() {
        return execution;
    }

    public void setExecution(NucleoStep execution) {
        this.execution = execution;
    }

    public int getVersion() {
        return version;
    }

    public void setVersion(int version) {
        this.version = version;
    }

    public int getTrack() {
        return track;
    }

    public void setTrack(int track) {
        this.track = track;
    }

    public int getRetries() {
        return retries;
    }

    public void setRetries(int retries) {
        this.retries = retries;
    }

    public long getTimeTrack() {
        return timeTrack;
    }

    public void setTimeTrack(long timeTrack) {
        this.timeTrack = timeTrack;
    }

    public Stack<Object[]> getTimeExecutions() {
        return timeExecutions;
    }

    public void setTimeExecutions(Stack<Object[]> timeExecutions) {
        this.timeExecutions = timeExecutions;
    }


}
