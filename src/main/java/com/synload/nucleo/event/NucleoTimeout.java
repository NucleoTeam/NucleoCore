package com.synload.nucleo.event;

import com.synload.nucleo.hub.Hub;

import java.util.TreeMap;

public class NucleoTimeout implements Runnable {
    private Hub hub;
    private final static int maxRetries = 4;
    private final static long loopTimer = 1000;
    private NucleoData data;

    public NucleoTimeout(Hub hub, NucleoData data) {
        this.data = data;
        this.hub = hub;
    }

    public void run() {
        try {
            //System.out.println("Starting timeout "+data.getChain()[data.getLink()]);
            Thread.sleep(loopTimer, 0);
            if (hub.getResponders().containsKey(data.getRoot().toString())) {
                synchronized (hub.getTimeouts()) {
                    hub.getTimeouts().remove(data.getRoot().toString());
                }
                int retries = data.getRetries();
                if (retries >= maxRetries) {
                    NucleoResponder responder = hub.getResponders().get(data.getRoot().toString());
                    hub.getResponders().remove(data.getRoot().toString());
                    data.getChainBreak().setBreakChain(true);
                    data.getChainBreak().getBreakReasons().add("Timeout on latest topic call");
                    data.getExecution().setEnd(System.currentTimeMillis());
                    hub.log("timeout", data);
                    responder.run(data);
                    return;
                }
                if (data.getTrack() == 1) {
                    Thread timeout = new Thread(new NucleoTimeout(hub, data));
                    timeout.start();
                    synchronized (hub.getTimeouts()) {
                        hub.getTimeouts().put(data.getRoot().toString(), timeout);
                    }
                }
                data.setRetries(retries + 1);
                hub.log("incomplete", data);
                hub.trafficCurrentRoute(data);
            }
        } catch (Exception e) {
            //e.printStackTrace();
        }

    }
}