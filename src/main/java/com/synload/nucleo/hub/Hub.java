package com.synload.nucleo.hub;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.*;
import com.synload.nucleo.NucleoMesh;
import com.synload.nucleo.data.NucleoData;
import com.synload.nucleo.data.NucleoObject;
import com.synload.nucleo.event.*;
import com.synload.nucleo.loader.LoadHandler;
import org.reflections.Reflections;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class Hub {
    private EventHandler eventHandler = new EventHandler();
    private HashMap<String, NucleoResponder> responders = Maps.newHashMap();
    private HashMap<String, Thread> timeouts = Maps.newHashMap();
    private String bootstrap;
    public static ObjectMapper objectMapper = new ObjectMapper();
    private String uniqueName;
    private NucleoMesh mesh;
    private TrafficHandler trafficHandler = new TrafficHandler();
    protected static final Logger logger = LoggerFactory.getLogger(Hub.class);


    public Hub(NucleoMesh mesh, String uniqueName, String elasticServer, int elasticPort) {
        this.uniqueName = uniqueName;
        this.mesh = mesh;
        //esPusher = new ElasticSearchPusher(elasticServer, elasticPort, "http");
        //new Thread(esPusher).start();
    }

    public NucleoData constructNucleoData(String chain, NucleoObject objects) {
        logger.debug("Constructing request");
        NucleoData data = new NucleoData();
        data.setObjects(objects);
        data.setOrigin(uniqueName);
        data.buildChains(chain);
        return data;
    }

    public NucleoData constructNucleoData(String[] chains, NucleoObject objects) {
        logger.debug("Constructing request");
        NucleoData data = new NucleoData();
        data.setObjects(objects);
        data.setTimeTrack(System.currentTimeMillis());
        data.setOrigin(uniqueName);
        data.buildChains(chains);
        return data;
    }

    public void log(String state, NucleoData data) {
        if (data.getTrack() == 1) {
            data.setVersion(data.getVersion() + 1);
            push(constructNucleoData(new String[]{"_watch." + state}, new NucleoObject() {{
                set("root", new NucleoData(data));
            }}), new NucleoResponder() {
                @Override
                public void run(NucleoData returnedData) {
                }
            }, false);
        }
    }

    public void nextChain(NucleoData data) {
        int onChain = data.getOnChain();
        if(data.getTrack()!=0)
            try {
                logger.debug("before: "+new ObjectMapper().writeValueAsString(data));
            } catch (Exception e) {
                e.printStackTrace();
            }
        List<NucleoData> datas = trafficHandler.getNext(data);
        if(data.getTrack()!=0) {
            try {
                logger.debug("next: " + new ObjectMapper().writeValueAsString(datas));
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        if (datas != null) {
            logger.debug(data.getRoot().toString() + ": data parts " + datas.size());
            datas.stream().forEach(x -> {
                if(x.currentChain()==null){
                    if (data.getTrack()==1) logger.debug(x.currentChainString() + " - sending to origin to re-assemble");
                    trafficRoute(null, x);
                }else {
                    if (onChain != x.getOnChain() && onChain > -1 && data.getChainList().get(onChain).getParallelChains().size() > 0) {
                        if (data.getTrack()==1) logger.debug(x.currentChainString() + ": sending to leader to re-assemble");
                        mesh.geteManager().leader(x.currentChainString(), x);
                    } else {
                        if (x.getChainList().get(x.getOnChain()).getParallelChains().size() > 0) {
                            logger.debug(x.currentChain() + ": routing to complete parallel");
                        } else {
                            logger.debug(x.currentChain() + ": routing to complete chain");
                        }
                        trafficRoute(x, data);
                    }
                }
            });
        }
    }

    public void trafficCurrentRoute(NucleoData data) {
        trafficRoute(data, null);
    }

    public void trafficRoute(NucleoData data, NucleoData originalData) {
        if (data == null) {
            logger.debug(originalData.getRoot().toString() + " - sending back to origin");
            sendToMesh("nucleo.client." + originalData.getOrigin(), originalData);
        } else {
            String topic = data.currentChainString();
            if (eventHandler.getChainToMethod().containsKey(topic)) {
                logger.debug(data.getRoot().toString() + " - executing locally " + topic);
                handle(this, data, topic);
            } else {
                logger.debug(data.getRoot().toString() + " - sending " + topic);
                sendToMesh(topic, data);
            }
        }
    }

    public void sendToMesh(String topic, NucleoData data) {
        //data.markTime("Queue Done, sending to round robin");
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
        nextChain(data);
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

    public void handle(Hub hub, NucleoData data, String topic) {
        new Thread(() -> trafficHandler.processParallel(data, new TrafficExecutor(hub, data, topic))).start();
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

    public TrafficHandler getTrafficHandler() {
        return trafficHandler;
    }

    public void setTrafficHandler(TrafficHandler trafficHandler) {
        this.trafficHandler = trafficHandler;
    }
}
