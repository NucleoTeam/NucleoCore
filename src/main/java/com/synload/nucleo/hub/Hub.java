package com.synload.nucleo.hub;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.*;
import com.synload.nucleo.NucleoMesh;
import com.synload.nucleo.event.*;
import com.synload.nucleo.loader.LoadHandler;
import org.reflections.Reflections;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Method;
import java.util.*;

public class Hub {
    private EventHandler eventHandler = new EventHandler();
    private HashMap<String, NucleoResponder> responders = Maps.newHashMap();
    private HashMap<String, Thread> timeouts = Maps.newHashMap();
    private String bootstrap;
    public static ObjectMapper objectMapper = new ObjectMapper();
    private String uniqueName;
    private NucleoMesh mesh;
    private Map<String, List<NucleoData>> parallelParts = Maps.newLinkedHashMap();
    protected static final Logger logger = LoggerFactory.getLogger(Hub.class);


    public Hub(NucleoMesh mesh, String uniqueName, String elasticServer, int elasticPort) {
        this.uniqueName = uniqueName;
        this.mesh = mesh;
        //esPusher = new ElasticSearchPusher(elasticServer, elasticPort, "http");
        //new Thread(esPusher).start();
    }

    public NucleoData constructNucleoData(String chain, TreeMap<String, Object> objects) {
        logger.debug("Constructing request");
        NucleoData data = new NucleoData();
        data.setObjects(objects);
        data.setOrigin(uniqueName);
        data.setOnChain(0);
        data.buildChains(chain);
        return data;
    }

    public NucleoData constructNucleoData(String[] chains, TreeMap<String, Object> objects) {
        logger.debug("Constructing request");
        NucleoData data = new NucleoData();
        data.setObjects(objects);
        data.setTimeTrack(System.currentTimeMillis());
        data.setOrigin(uniqueName);
        data.setOnChain(0);
        data.buildChains(chains);
        return data;
    }
    public void log(String state, NucleoData data){
        if (data.getTrack() == 1) {
            data.setVersion(data.getVersion() + 1);
            push(constructNucleoData(new String[]{"_watch."+state}, new TreeMap<String, Object>() {{
                put("root", new NucleoData(data));
            }}), new NucleoResponder() {
                @Override
                public void run(NucleoData returnedData) {
                }
            }, false);
        }
    }

    public void nextChain(NucleoData data){
        int onChain = data.getOnChain();
        List<NucleoData> datas = data.getNext();
        if(datas!=null){
            datas.stream().forEach(x->{
                if(onChain<x.getOnChain() &&
                    data.getChainList().get(onChain).getParallelChains().size()>0){
                    logger.debug(x.getRoot().toString() + " - sending to leader to re-assemble");
                    mesh.geteManager().leader(x.currentChain(), x);
                }else{
                    trafficRoute(x, data);
                }
            });
        } else {
            trafficRoute(null, data);
        }
    }
    public void currentChain(NucleoData data){
        trafficRoute(data, null);
    }
    public void trafficRoute(NucleoData data, NucleoData originalData){
        if(data == null) {
            logger.debug(originalData.getRoot().toString() + " - sending back to origin");
            sendToMesh("nucleo.client." + originalData.getOrigin(), originalData);
        } else {
            String topic = data.currentChain();
            logger.debug(data.getRoot().toString() + " - sending " +topic);
            if (eventHandler.getChainToMethod().containsKey(topic)) {
                handle(this, data, topic);
            } else {
                sendToMesh(topic, data);
            }
        }
    }

    public void sendToMesh(String topic, NucleoData data) {
        data.markTime("Queue Done, sending to round robin");
        mesh.geteManager().robin(topic, new NucleoData(data));
    }

    public void push(NucleoData data, NucleoResponder responder, boolean allowTracking) {
        responders.put(data.getRoot().toString(), responder);
        if (allowTracking) {
            Thread timeout = new Thread(new NucleoTimeout(this, data));
            timeout.start();
            synchronized (timeouts) {
                timeouts.put(data.getRoot().toString(), timeout);
            }
            log("incomplete", data);
        } else {
            data.setTrack(0);
        }
        trafficRoute(data, null);
    }

    public void register(String servicePackage) {
        try {
            Reflections reflect = new Reflections(servicePackage);
            Set<Class<?>> classes = reflect.getTypesAnnotatedWith(NucleoClass.class);
            LoadHandler.getMethods(classes.toArray()).forEach((m) -> getEventHandler().registerMethod(m));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public class Executor implements Runnable {
        public Hub hub;
        public NucleoData data;
        public String topic;


        public Executor(Hub hub, NucleoData data, String topic) {
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

        public boolean checkParallel(int para, int direction){
            int previousChain = data.getOnChain()+direction;
            NucleoChain chain = data.getChainList().get(previousChain);
            if(!chain.getParallelChains().get(chain.getParallel()).isComplete()){
                return false;
            }
            String root = data.getRoot().toString();
            if(!hub.parallelParts.containsKey(root)){
                hub.parallelParts.put(root, new ArrayList<>());
            }
            List<NucleoData> paraParts = hub.parallelParts.get(root);
            paraParts.add(data);

            if(paraParts.size()!=para) {
                logger.debug(root + " waiting for all parts");
                return true;
            }

            hub.parallelParts.remove(root);
            NucleoData finalPart = null;
            for(NucleoData part : paraParts){
                NucleoChain chainPart = part.getChainList().get(part.getOnChain()+direction);
                int parallel = chainPart.getParallel();
                chainPart = chainPart.getParallelChains().get(parallel);
                try {
                    logger.debug(new ObjectMapper().writeValueAsString(chainPart));
                }catch (Exception e){e.printStackTrace();}
                if(finalPart!=null) {
                    if (finalPart!=part){
                        logger.debug("combining " + chainPart.getChainString().toString());
                        finalPart.getObjects().putAll(part.getObjects());
                        int stepStart = chainPart.stepStart;
                        if(stepStart!=-1) {
                            chainPart.setStepStart(finalPart.getSteps().size());
                            for (int i = stepStart; i < part.getSteps().size(); i++) {
                                finalPart.getSteps().add(part.getSteps().get(i));
                            }
                        }
                        finalPart.getChainList().get(part.getOnChain() + direction).getParallelChains().set(parallel, chainPart);
                        chainPart.setRecombined(true);
                        try {
                            logger.debug(new ObjectMapper().writeValueAsString(chainPart));
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }else{
                        logger.info("HOW THE FUCK");
                    }
                }else{
                    logger.debug("main part combining "+chainPart.getChainString().toString());
                    chainPart.setRecombined(true);
                    finalPart = part;
                }
            }
            finalPart.getChainList().get(finalPart.getOnChain()+direction).setRecombined(true);
            data = finalPart;
            logger.debug(root + " all parts received, combining");
            return false;
        }

        public void run() {
            try {
                data.markTime("Start Execution on " + uniqueName);
                if (topic.startsWith("nucleo.client.")) {

                    // check for parallel
                    int para = data.parallel( 0);
                    if(para>0 && checkParallel(para, 0)){
                        return;
                    }

                    // handle ping requests for uptime
                    if (data.getObjects().containsKey("_ping")) {
                        Stack<String> hosts = (Stack<String>) data.getObjects().get("ping");
                        if (hosts != null && !hosts.isEmpty()) {
                            String host = hosts.pop();
                            //System.out.println("going to: nucleo.client." + host);
                            data.markTime("Execution Complete");
                            sendToMesh("nucleo.client." + host, data);
                            return;
                        } else {
                            //esPusher.add(data);
                            data.getObjects().remove("_ping");
                            data.markTime("Execution Complete");
                            sendToMesh("nucleo.client." + data.getOrigin(), data);
                            //System.out.println("ping going home!");
                            return;
                        }
                    }

                    // handle routing of request through this service
                    if (data.getObjects().containsKey("_route")) {
                        List<String> hosts = (List<String>) data.getObjects().get("_route");
                        if (hosts != null && !hosts.isEmpty()) { // route not complete
                            String host = hosts.remove(0);
                            if(host.equals(uniqueName)) {
                                data.getObjects().remove("_route");
                                data.markTime("Route Complete");
                                currentChain(data);
                                return;
                            }
                            //System.out.println("going to: nucleo.client." + host);
                            data.markTime("Sending to "+host);
                            sendToMesh("nucleo.client." + host, data);
                            return;
                        }else{
                            data.getObjects().remove("_route");
                            data.markTime("Route Complete");
                            currentChain(data);
                            return;
                        }
                    }

                    // Finish request and execute the final responder
                    NucleoResponder responder = responders.get(data.getRoot().toString());
                    if (responder != null) {
                        responders.remove(data.getRoot().toString());
                        Thread timeout = timeouts.get(data.getRoot().toString());
                        if (timeout != null) {
                            timeout.interrupt();
                            timeouts.remove(data.getRoot().toString());
                        }
                        data.getExecution().setEnd(System.currentTimeMillis());
                        //esPusher.add(data);
                        data.markTime("Execution Complete");
                        log("complete", data);
                        responder.run(data);
                        //System.out.println("response: " + data.markTime() + "ms");
                        return;
                    }
                } else if (eventHandler.getChainToMethod().containsKey(topic)) {

                    int para = data.parallel(-1);
                    if(para>0 && checkParallel(para, -1)){
                        return;
                    }

                    data.setSteps();

                    logger.debug(data.getRoot().toString() + " - processing " + topic);
                    Object[] methodData = eventHandler.getChainToMethod().get(topic);
                    NucleoStep timing = new NucleoStep(topic, System.currentTimeMillis());
                    if (methodData[2] != null) {
                        Set<String> missingChains;
                        if ((missingChains = verifyPrevious((Set<String>) methodData[2])) != null) {
                            timing.setEnd(System.currentTimeMillis());
                            data.getChainBreak().setBreakChain(true);
                            data.getChainBreak().getBreakReasons().add("Missing required chains " + missingChains + "!");
                            data.getSteps().add(timing);
                            //esPusher.add(data);
                            data.markTime("Execution Complete");
                            log("complete", data);
                            sendToMesh("nucleo.client." + data.getOrigin(), data);
                            return;
                        }
                    }
                    data.markTime("Verified Chain Requirements");
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
                                data.markTime("Execution Complete");
                                log("incomplete", data);

                                sendToMesh("nucleo.client." + data.getOrigin(), data);
                                return;
                            }
                            logger.debug(data.getRoot().toString() + " - processed " + topic);
                            timing.setEnd(System.currentTimeMillis());
                            data.getSteps().add(timing);
                            //esPusher.add(data);
                            data.markTime("Execution Complete");
                            log("incomplete", data);
                            nextChain(data);
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
    }

    public void handle(Hub hub, NucleoData data, String topic) {
        new Thread(new Executor(hub, data, topic)).start();
    }

    public EventHandler getEventHandler() {
        return eventHandler;
    }

    public void setEventHandler(EventHandler eventHandler) {
        this.eventHandler = eventHandler;
    }

    public HashMap<String, NucleoResponder> getResponders() {
        return responders;
    }

    public void setResponders(HashMap<String, NucleoResponder> responders) {
        this.responders = responders;
    }


    public String getBootstrap() {
        return bootstrap;
    }

    public void setBootstrap(String bootstrap) {
        this.bootstrap = bootstrap;
    }

    public HashMap<String, Thread> getTimeouts() {
        return timeouts;
    }

    public void setTimeouts(HashMap<String, Thread> timeouts) {
        this.timeouts = timeouts;
    }

    public static ObjectMapper getObjectMapper() {
        return objectMapper;
    }

    public static void setObjectMapper(ObjectMapper objectMapper) {
        Hub.objectMapper = objectMapper;
    }

    public String getUniqueName() {
        return uniqueName;
    }

    public void setUniqueName(String uniqueName) {
        this.uniqueName = uniqueName;
    }

    public NucleoMesh getMesh() {
        return mesh;
    }

    public void setMesh(NucleoMesh mesh) {
        this.mesh = mesh;
    }
}
