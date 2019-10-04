package com.synload.nucleo.event;

import java.util.*;

public class NucleoData implements Cloneable  {
  private UUID root;
  private List<String[]> chainList = new ArrayList<>();
  private String origin;
  private int link;
  private List<NucleoStep> steps = new ArrayList<>();
  private NucleoStep execution = new NucleoStep();
  private int onChain;
  private int track = 1;
  private Stack<Object[]> timeExecutions = new Stack<>();
  private long timeTrack = System.currentTimeMillis();
  private int retries = 0;
  private int version=0;
  private TreeMap<String, Object> objects;
  private NucleoChainStatus chainBreak = new NucleoChainStatus();

  public NucleoData(){
    root = UUID.randomUUID();
    getExecution().setStart(System.currentTimeMillis());
  }

  public long markTime(){
    long total = System.currentTimeMillis() - timeTrack;
    //timeTrack = System.currentTimeMillis();
    //timeExecutions.add(new Object[]{total});
    return total;
  }
  public long markTime(String message){
    long total = System.currentTimeMillis() - timeTrack;
    //timeTrack = System.currentTimeMillis();
    //timeExecutions.add(new Object[]{message, total});
    return total;
  }
  public TreeMap<String, Object> getObjects() {
    return objects;
  }

  public void setObjects(TreeMap<String, Object> objects) {
    this.objects = objects;
  }

  public int getLink() {
    return link;
  }

  public void setLink(int link) {
    this.link = link;
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

  public List<String[]> getChainList() {
    return chainList;
  }

  public void setChainList(List<String[]> chainList) {
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
  public Object clone() throws  CloneNotSupportedException {
    return super.clone();
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
