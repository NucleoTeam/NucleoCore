package com.synload.nucleo.event;

import java.util.TreeMap;
import java.util.UUID;

public class NucleoData {
  private UUID uuid;
  private UUID root;
  private String[] chain;
  private String origin;
  private int link;
  private TreeMap<String, Object> objects;
  private boolean chainBreak = false;

  public NucleoData(){
    root = UUID.randomUUID();
  }

  public UUID getUuid() {
    return uuid;
  }

  public void setUuid(UUID uuid) {
    this.uuid = uuid;
  }

  public String[] getChain() {
    return chain;
  }

  public void setChain(String[] chain) {
    this.chain = chain;
  }

  public TreeMap<String, Object> getObjects() {
    return objects;
  }

  public void setObjects(TreeMap<String, Object> objects) {
    this.objects = objects;
  }

  public boolean isChainBreak() {
    return chainBreak;
  }

  public void setChainBreak(boolean chainBreak) {
    this.chainBreak = chainBreak;
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
}
