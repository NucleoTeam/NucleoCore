package com.synload.nucleo.event;

import java.util.ArrayList;
import java.util.List;
import java.util.TreeMap;
import java.util.UUID;

public class NucleoData {
  private UUID root;
  private List<String[]> chainList = new ArrayList<>();
  private String origin;
  private int link;
  private long start;
  private long end;
  private int onChain;
  private TreeMap<String, Object> objects;
  private NucleoChainStatus chainBreak = new NucleoChainStatus();

  public NucleoData(){
    root = UUID.randomUUID();
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
  }
}
