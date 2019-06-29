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
      //System.out.println("Starting timeout "+data.getChain()[data.getLink()]);
      Thread.sleep(1000);
      if (hub.getResponders().containsKey(data.getRoot().toString())) {
        NucleoResponder responder = hub.getResponders().get(data.getRoot().toString());
        hub.getResponders().remove(data.getRoot().toString());
        hub.getTimeouts().remove(data.getRoot().toString());
        //System.out.println("Ending timeout "+data.getChain()[data.getLink()]+" and responding");
        data.getChainBreak().setBreakChain(true);
        data.getChainBreak().getBreakReasons().add("Timeout on latest topic call");
        data.getExecution().setEnd(System.currentTimeMillis());
        hub.push(hub.constructNucleoData(new String[]{"timeouts"}, new TreeMap<String, Object>() {{
          put("root", data.getRoot());
        }}), new NucleoResponder() {
          @Override
          public void run(NucleoData returnedData) {
          }
        });
        responder.run(data);
      }
    } catch (Exception e) {
      //e.printStackTrace();
    }

  }
}