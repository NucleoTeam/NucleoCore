package com.synload.nucleo.hub;

import com.google.common.collect.Sets;
import com.synload.nucleo.NucleoMesh;
import com.synload.nucleo.data.NucleoData;
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
            if (topic == null) {
                // Finish request and execute the final responder
                NucleoResponder responder = hub.getResponders().get(data.getRoot().toString());
                if (responder != null) {
                    hub.getResponders().remove(data.getRoot().toString());
                    data.getExecution().setEnd(System.currentTimeMillis());
                    //esPusher.add(data);
                    //data.markTime("Execution Complete");
                    hub.log("complete", data);
                    responder.run(data);
                    //System.out.println("response: " + data.markTime() + "ms");
                    return;
                }
            } else if (hub.getMesh().getEventHandler().getChainToMethod().containsKey(topic)) {
                data.setStepsStart();
                logger.debug(data.getRoot().toString() + " - processing " + topic);
                Object[] methodData = hub.getMesh().getEventHandler().getChainToMethod(topic);
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
                        hub.sendRoot(data);
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
                NucleoResponder responder = data -> {
                    if (data.getChainBreak().isBreakChain()) {
                        timing.setEnd(System.currentTimeMillis());
                        data.getSteps().add(timing);
                        //esPusher.add(data);
                        //data.markTime("Execution Complete");
                        hub.log("incomplete", data);
                        hub.sendRoot(data);
                        return;
                    }
                    logger.debug(data.getRoot().toString() + " - processed " + topic);
                    timing.setEnd(System.currentTimeMillis());
                    data.getSteps().add(timing);
                    //esPusher.add(data);
                    //data.markTime("Execution Complete");
                    hub.log("incomplete", data);
                    hub.nextChain(data);
                };
                Object[] objects = new Object[method.getParameterCount()];
                Class[] classes = method.getParameterTypes();
                Class returnType = method.getReturnType();
                boolean executeResponder = true;

                data.getObjects().buildCurrentState(); // get current state of objects,

                for (int i = 0; i < classes.length; i++) {
                    if(classes[i] == NucleoData.class){
                        objects[i] = data;
                        continue;
                    }
                    if(classes[i] == NucleoResponder.class){
                        objects[i] = responder;
                        executeResponder = false;
                        continue;
                    }
                    if(classes[i] == NucleoMesh.class){
                        objects[i] = this.getHub().getMesh();
                        continue;
                    }
                    if(classes[i] == Hub.class){
                        objects[i] = this.getHub();
                        continue;
                    }
                    objects[i] = null;
                }
                if (executeResponder && returnType == NucleoData.class) {
                    try {
                        responder.run((NucleoData)method.invoke(obj, objects));
                    } catch (Exception e) {
                        e.printStackTrace();
                        data.getChainBreak().setBreakChain(true);
                        data.getChainBreak().getBreakReasons().add(e.getMessage());
                    }
                }else {
                    try {
                        method.invoke(obj, objects);
                    } catch (Exception e) {
                        e.printStackTrace();
                        data.getChainBreak().setBreakChain(true);
                        data.getChainBreak().getBreakReasons().add(e.getMessage());
                    }
                    if (executeResponder)
                        responder.run(data);
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
