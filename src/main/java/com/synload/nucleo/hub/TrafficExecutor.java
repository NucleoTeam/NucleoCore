package com.synload.nucleo.hub;

import com.google.common.collect.Sets;
import com.synload.nucleo.event.NucleoData;
import com.synload.nucleo.event.NucleoResponder;
import com.synload.nucleo.event.NucleoStep;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Method;
import java.util.List;
import java.util.Set;
import java.util.Stack;

public class TrafficExecutor {
    public Hub hub;
    public NucleoData data;
    public String topic;
    protected static final Logger logger = LoggerFactory.getLogger(TrafficExecutor.class);

    public TrafficExecutor(Hub hub, NucleoData data, String topic) {
        this.hub = hub;
        this.data = data;
        this.topic = topic;
    }


    public Set<String> verifyPrevious(Set<String> checkChains) {
        Set<String> previousChains = Sets.newLinkedHashSet();
        Set<String> checkChainsTMP = Sets.newLinkedHashSet();
        checkChainsTMP.addAll(checkChains);
        data.getSteps().stream().filter(s -> s.getEnd() > 0).forEach(s -> previousChains.add(s.getStep()));
        if (previousChains.containsAll(checkChainsTMP)) {
            return null;
        }
        //data.getChainBreak().getBreakReasons();
        checkChainsTMP.removeAll(previousChains);
        return checkChainsTMP;
    }

    public void handle() {
        try {
            //data.markTime("Start Execution on " + hub.getUniqueName());
            if (topic.startsWith("nucleo.client.")) {
                // handle ping requests for uptime
                logger.debug("ORIGIN RECEIVED");
                if (data.getObjects().containsKey("_ping")) {
                    Stack<String> hosts = (Stack<String>) data.getObjects().get("ping");
                    if (hosts != null && !hosts.isEmpty()) {
                        String host = hosts.pop();
                        //System.out.println("going to: nucleo.client." + host);
                        //data.markTime("Execution Complete");
                        hub.sendToMesh("nucleo.client." + host, data);
                        return;
                    } else {
                        //esPusher.add(data);
                        data.getObjects().remove("_ping");
                        //data.markTime("Execution Complete");
                        hub.sendToMesh("nucleo.client." + data.getOrigin(), data);
                        //System.out.println("ping going home!");
                        return;
                    }
                }

                // handle routing of request through this service
                if (data.getObjects().containsKey("_route")) {
                    List<String> hosts = (List<String>) data.getObjects().get("_route");
                    if (hosts != null && !hosts.isEmpty()) { // route not complete
                        String host = hosts.remove(0);
                        if (host.equals(hub.getUniqueName())) {
                            data.getObjects().remove("_route");
                            //data.markTime("Route Complete");
                            hub.trafficCurrentRoute(data);
                            return;
                        }
                        //System.out.println("going to: nucleo.client." + host);
                        //data.markTime("Sending to " + host);
                        hub.sendToMesh("nucleo.client." + host, data);
                        return;
                    } else {
                        data.getObjects().remove("_route");
                        //data.markTime("Route Complete");
                        hub.trafficCurrentRoute(data);
                        return;
                    }
                }

                // Finish request and execute the final responder
                NucleoResponder responder = hub.getResponders().get(data.getRoot().toString());
                if (responder != null) {
                    hub.getResponders().remove(data.getRoot().toString());
                    Thread timeout = hub.getTimeouts().get(data.getRoot().toString());
                    if (timeout != null) {
                        timeout.interrupt();
                        hub.getTimeouts().remove(data.getRoot().toString());
                    }
                    data.getExecution().setEnd(System.currentTimeMillis());
                    //esPusher.add(data);
                    //data.markTime("Execution Complete");
                    hub.log("complete", data);
                    responder.run(data);
                    //System.out.println("response: " + data.markTime() + "ms");
                    return;
                }
            } else if (hub.getEventHandler().getChainToMethod().containsKey(topic)) {

                data.setStepsStart();

                logger.debug(data.getRoot().toString() + " - processing " + topic);
                Object[] methodData = hub.getEventHandler().getChainToMethod().get(topic);
                NucleoStep timing = new NucleoStep(topic, System.currentTimeMillis());
                if (methodData[2] != null) {
                    Set<String> missingChains;
                    if ((missingChains = verifyPrevious((Set<String>) methodData[2])) != null) {
                        timing.setEnd(System.currentTimeMillis());
                        data.getChainBreak().setBreakChain(true);
                        data.getChainBreak().getBreakReasons().add("Missing required chains " + missingChains + "!");
                        data.getSteps().add(timing);
                        //esPusher.add(data);
                        //data.markTime("Execution Complete");
                        hub.log("complete", data);
                        hub.sendToMesh("nucleo.client." + data.getOrigin(), data);
                        return;
                    }
                }
                //data.markTime("Verified Chain Requirements");
                Object obj;
                if (methodData[0] instanceof Class) {
                    Class clazz = (Class) methodData[0];
                    obj = clazz.getDeclaredConstructor().newInstance();
                } else {
                    obj = methodData[0];
                }
                Method method = (Method) methodData[1];
                NucleoResponder responder = new NucleoResponder() {
                    public void run(NucleoData data) {
                        if (data.getChainBreak().isBreakChain()) {
                            timing.setEnd(System.currentTimeMillis());
                            data.getSteps().add(timing);
                            //esPusher.add(data);
                            //data.markTime("Execution Complete");
                            hub.log("incomplete", data);

                            hub.sendToMesh("nucleo.client." + data.getOrigin(), data);
                            return;
                        }
                        logger.debug(data.getRoot().toString() + " - processed " + topic);
                        timing.setEnd(System.currentTimeMillis());
                        data.getSteps().add(timing);
                        //esPusher.add(data);
                        //data.markTime("Execution Complete");
                        hub.log("incomplete", data);
                        hub.nextChain(data);
                    }
                };
                int len = method.getParameterTypes().length;
                if (len > 0) {
                    if (method.getParameterTypes()[0] == NucleoData.class && len == 1) {
                        try {
                            method.invoke(obj, data);
                        } catch (Exception e) {
                            data.getChainBreak().setBreakChain(true);
                            data.getChainBreak().getBreakReasons().add(e.getMessage());
                        }
                        responder.run(data);
                    } else if (method.getParameterTypes()[0] == NucleoData.class && len == 2 && method.getParameterTypes()[1] == NucleoResponder.class) {
                        try {
                            method.invoke(obj, data, responder);
                        } catch (Exception e) {
                            data.getChainBreak().setBreakChain(true);
                            data.getChainBreak().getBreakReasons().add(e.getMessage());
                        }
                    }
                } else {
                    try {
                        method.invoke(obj);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                    responder.run();
                }
            } else {
                //System.out.println("Topic or responder not found: " + topic);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public Hub getHub() {
        return hub;
    }

    public void setHub(Hub hub) {
        this.hub = hub;
    }

    public NucleoData getData() {
        return data;
    }

    public void setData(NucleoData data) {
        this.data = data;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }
}
