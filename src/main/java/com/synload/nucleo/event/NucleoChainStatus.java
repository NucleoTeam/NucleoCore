package com.synload.nucleo.event;

import java.util.HashSet;
import java.util.LinkedList;
import java.util.Set;

public class NucleoChainStatus {
  private boolean breakChain = false;
  private Set<String> breakReasons = new HashSet<>();

  public NucleoChainStatus(NucleoChainStatus status) {
    this.breakChain = status.breakChain;
    this.breakReasons = new HashSet<>(status.breakReasons);
  }

  public NucleoChainStatus() {
  }

  public boolean isBreakChain() {
    return breakChain;
  }

  public void setBreakChain(boolean breakChain) {
    this.breakChain = breakChain;
  }

  public Set<String> getBreakReasons() {
    return breakReasons;
  }

  public void setBreakReasons(Set<String> breakReasons) {
    this.breakReasons = breakReasons;
  }
}
