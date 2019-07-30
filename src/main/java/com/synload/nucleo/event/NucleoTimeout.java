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
      Thread.sleep(500);
      if (hub.getResponders().containsKey(data.getRoot().toString())) {
        hub.getTimeouts().remove(data.getRoot().toString());
        /*NucleoResponder responder = hub.getResponders().get(data.getRoot().toString());
        hub.getResponders().remove(data.getRoot().toString());
        data.getChainBreak().setBreakChain(true);
        data.getChainBreak().getBreakReasons().add("Timeout on latest topic call");
        data.getExecution().setEnd(System.currentTimeMillis());*/
        if (data.getTrack() == 1) {
          Thread timeout = new Thread(new NucleoTimeout(hub, data));
          timeout.start();
          hub.getTimeouts().put(data.getRoot().toString(), timeout);
        }
        hub.getQueue().add(new Object[]{data.getChainList().get(data.getOnChain())[data.getLink()], data});
        /*if (data.getTrack() == 1) {
          hub.push(hub.constructNucleoData(new String[]{"_watch.timeout"}, new TreeMap<String, Object>() {{
            put("root", data.getRoot());
          }}), new NucleoResponder() {
            @Override
            public void run(NucleoData returnedData) {
            }
          }, false);
        }
        responder.run(data);*/
      }
    } catch (Exception e) {
      //e.printStackTrace();
    }

  }
}