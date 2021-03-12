package com.synload.nucleo.data;

import com.synload.nucleo.chain.ChainExecution;
import com.synload.nucleo.chain.NucleoChainStatus;
import com.synload.nucleo.chain.path.Run;
import com.synload.nucleo.chain.path.SingularRun;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.*;

public class NucleoData implements Cloneable, Serializable {
    protected static final Logger logger = LoggerFactory.getLogger(NucleoData.class);
    private UUID root;
    private String origin;
    private List<NucleoStep> steps = new ArrayList<>();
    private ChainExecution chainExecution = null;
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
        //getChainExecution().setStart(System.currentTimeMillis());
    }

    public NucleoData(NucleoData data, ChainExecution execution) {
        this.root = data.root;
        this.origin = data.origin;
        //data.steps.stream().forEach(x -> this.steps.add(new NucleoStep(x)));
        this.chainExecution = execution;
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

    public NucleoData(NucleoData data) {
        this.root = data.root;
        this.origin = data.origin;
        //data.steps.stream().forEach(x -> this.steps.add(new NucleoStep(x)));
        this.chainExecution = data.getChainExecution();
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

    public boolean onParallelStep(){
        return chainExecution.getCurrent().isParallel();
    }

    public Run currentChain() {
        return chainExecution.getCurrent();
    }
    public Set<Run> currentParentChains() {
        return currentChain().allParents();
    }

    public Set<Run> previousParentChain() {
        return currentChain().getParents();
    }

    public String currentChainString() {
        if(chainExecution.getCurrent().getClass() == SingularRun.class){
            return ((SingularRun)chainExecution.getCurrent()).getChain();
        }
        return null;
    }

    /*public void setStepsStart() {
        NucleoChain chain = currentChain();
        if (chain.stepStart == -1) {
            chain.setStepStart(steps.size());
        }
    }*/

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

    public int getOnChain() {
        return onChain;
    }

    public void setOnChain(int onChain) {
        this.onChain = onChain;
    }

    /*public List<NucleoStep> getSteps() {
        return steps;
    }

    public void setSteps(List<NucleoStep> steps) {
        this.steps = steps;
    }
*/
    public ChainExecution getChainExecution() {
        return chainExecution;
    }

    public void setChainExecution(ChainExecution chainExecution) {
        this.chainExecution = chainExecution;
    }

    public List<NucleoStep> getSteps() {
        return steps;
    }

    public void setSteps(List<NucleoStep> steps) {
        this.steps = steps;
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
