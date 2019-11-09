package com.synload.nucleo.event;

import com.synload.nucleo.hub.Hub;
import org.apache.log4j.LogManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class NucleoData implements Cloneable  {
  private UUID root;
  private List<NucleoChain> chainList = new ArrayList<>();
  private String origin;
  private List<NucleoStep> steps = new ArrayList<>();
  private NucleoStep execution = new NucleoStep();
  private int onChain=0;
  private int track = 1;
  private Stack<Object[]> timeExecutions = new Stack<>();
  private long timeTrack = System.currentTimeMillis();
  private int retries = 0;
  private int version=0;
  private TreeMap<String, Object> objects;
  private NucleoChainStatus chainBreak = new NucleoChainStatus();
  protected static final Logger logger = LoggerFactory.getLogger(NucleoData.class);

  public NucleoData(){
    root = UUID.randomUUID();

    getExecution().setStart(System.currentTimeMillis());
  }

  public NucleoData(NucleoData data) {
    this.root = data.root;
    data.chainList.stream().forEach(x->this.getChainList().add(new NucleoChain(x)));
    this.origin = data.origin;
    data.steps.stream().forEach(x->this.steps.add(new NucleoStep(x)));
    this.execution = new NucleoStep(data.execution);
    this.onChain = data.onChain;
    this.track = data.track;
    this.timeExecutions = (Stack<Object[]>)data.timeExecutions.clone();
    this.timeTrack = data.timeTrack;
    this.retries = data.retries;
    this.version = data.version;
    this.objects = new TreeMap<>(data.objects);
    this.chainBreak = new NucleoChainStatus(data.chainBreak);
  }

  public long markTime(){
    //long total = System.currentTimeMillis() - timeTrack;
    //timeTrack = System.currentTimeMillis();
    //timeExecutions.add(new Object[]{total});
    return 0;
  }
  public long markTime(String message){
    //long total = System.currentTimeMillis() - timeTrack;
    //timeTrack = System.currentTimeMillis();
    //timeExecutions.add(new Object[]{message, total});
    return 0;
  }

  public void buildChains(String chain){
    if(chain==null || chain.length()==0)
      return;
    if(chain.contains("[")){
      String[] parallels = chain.substring(1,chain.length()-1).split("/");
      NucleoChain chainParallel = new NucleoChain();
      logger.debug(this.getRoot().toString() + " - " + "Parallel chain");
      for(String parallel : parallels){
        NucleoChain parallelInner = new NucleoChain();
        parallelInner.setChainString(parallel.split("\\."));
        chainParallel.getParallelChains().add(parallelInner);
      }
      this.getChainList().add(chainParallel);
    } else {
      NucleoChain singleChain = new NucleoChain();
      singleChain.setChainString(chain.split("\\."));
      logger.debug(this.getRoot().toString() + " - " + "Single chain request");
      this.getChainList().add(singleChain);
    }
  }
  public void buildChains(String[] chains){
    for(String chain : chains){
      buildChains(chain);
    }
  }

  public String depthChainString(NucleoChain chain){
    chain = depthChain(chain);
    if(chain!=null){
      String chainStr = "";
      for(int x=0;x<=chain.part;x++){
        chainStr += ((chainStr.isEmpty())?"":".") + chain.getChainString()[x];
      }
      //logger.info(this.getRoot().toString() + " - " + "current chain " + chainStr);
      return chainStr;
    }
    return null;
  }
  public NucleoChain depthChain(){
    return depthChain(chainList.get(onChain));
  }
  public NucleoChain depthChain(NucleoChain chain){
    if(chain.getChainString()!=null){
      return chain;
    } else if(chain.getParallelChains().size()>0 && chain.getParallelChains().size()>chain.getParallel()){
      return depthChain(chain.getParallelChains().get(chain.getParallel()));
    }
    return null;
  }

  public int parallel(int direction){
    int total = 0;
    for(int x = this.getOnChain() + direction;x>=0;x--) {
      NucleoChain previousChain = this.getChainList().get(x);
      if(previousChain.getParallelChains().size()==0){
        return total;
      }
      total = (total==0)?previousChain.getParallelChains().size():total * previousChain.getParallelChains().size();
    }
    return total;
  }

  public String currentChain(){
    if(this.getChainList().size()==0 || this.getChainList().size()<=this.onChain)
      return null;
    NucleoChain chain = this.getChainList().get(this.onChain);
    return depthChainString(chain);
  }
  public void setSteps(){
    NucleoChain chain = depthChain();
    if(chain.stepStart==-1) {
      chain.setStepStart(steps.size());
    }
  }

  public List<NucleoData> getNext(){
    if(this.getChainList().size()==0 || this.getChainList().size()<=this.onChain)
      return null;
    NucleoChain chain = this.getChainList().get(this.onChain);
    NucleoChain currentChain = depthChain(chain);
    List<NucleoData> nextDatas = new ArrayList<>();
    if(currentChain.getChainString()!=null){
      if(currentChain.getChainString().length>currentChain.getPart()+1){
        currentChain.setPart(currentChain.getPart()+1);
        logger.debug(this.getRoot().toString() + " - " + "moving to next part in chain "+currentChain.getPart());
        nextDatas.add(new NucleoData(this));
        logger.debug(this.getRoot().toString() + " - " + "size " + nextDatas.size());
        logger.debug(this.getRoot().toString() + " - " + this.currentChain());
        return nextDatas;
      } else {
        currentChain.setComplete(true);
        if(this.getChainList().size()>this.getOnChain()+1){
          this.setOnChain(this.getOnChain()+1);
          logger.debug(this.getRoot().toString() + " - " + this.currentChain());
          logger.debug(this.getRoot().toString() + " - " + "moving to next chain " + this.getOnChain());
          if(this.getChainList().get(this.getOnChain()).getChainString() != null){
            logger.debug(this.getRoot().toString() + " - " + "single chain for next");
            nextDatas.add(new NucleoData(this));
            logger.debug(this.getRoot().toString() + " - " + "size " + nextDatas.size());
            return nextDatas;
          }else{
            logger.debug(this.getRoot().toString() + " - " + "parallel chain for next");
            int size = this.getChainList().get(this.getOnChain()).getParallelChains().size();
            for(int i=0;i<size;i++){
              logger.debug(this.getRoot().toString() + " - " + "parallel chain " + i);
              NucleoData newData = new NucleoData(this);
              newData.getChainList().get(this.getOnChain()).setParallel(i);
              nextDatas.add(newData);
            }
            logger.debug(this.getRoot().toString() + " - " + "size " + nextDatas.size());
            return nextDatas;
          }
        }
      }
    }
    logger.debug(this.getRoot().toString() + " - " + "end of chain");
    return null; // null means end of chainlist
  }

  public TreeMap<String, Object> getObjects() {
    return objects;
  }

  public void setObjects(TreeMap<String, Object> objects) {
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
  public NucleoData clone()  {
    try {
      return (NucleoData) super.clone();
    }catch (Exception e){
      return null;
    }
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
