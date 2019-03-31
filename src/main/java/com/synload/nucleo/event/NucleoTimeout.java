package com.synload.nucleo.event;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.synload.nucleo.hub.Hub;

import java.util.TreeMap;

public class NucleoTimeout implements Runnable {
  private Hub hub;
  private NucleoData data;
  public NucleoTimeout(Hub hub, NucleoData data) {
    this.data = data;
    this.hub = hub;
  }
  public void run() {
    try {
      System.out.println("Starting timeout "+data.getChain()[data.getLink()]);
      Thread.sleep(300);
      if (hub.getResponders().containsKey(data.getRoot().toString())) {
        NucleoResponder responder = hub.getResponders().get(data.getRoot().toString());
        hub.getResponders().remove(data.getRoot().toString());
        System.out.println("Ending timeout "+data.getChain()[data.getLink()]+" and responding");
        responder.run(data);
      }else {
        System.out.println("Ending timeout "+data.getChain()[data.getLink()]+" without responder");
      }
    } catch (Exception e) {
      e.printStackTrace();
    }

  }
}