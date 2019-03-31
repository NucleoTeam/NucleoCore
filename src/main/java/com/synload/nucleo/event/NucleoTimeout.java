package com.synload.nucleo.event;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.TreeMap;

public class NucleoTimeout implements Runnable {
  private TreeMap<String, NucleoResponder> responders;
  private NucleoData data;
  public NucleoTimeout(TreeMap<String, NucleoResponder> responders, NucleoData data) {
    this.data = data;
    this.responders = responders;
  }
  public void run() {
    try {
      Thread.sleep(5000);
      if (responders.containsKey(data.getUuid().toString())) {
        System.out.println(new ObjectMapper().writeValueAsString(data));
        NucleoResponder responder = responders.get(data.getUuid().toString());
        responders.remove(data.getUuid().toString());
        responder.run(data);
      }
    } catch (Exception e) {
      e.printStackTrace();
    }

  }
}