package com.synload.nucleo.hub;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.*;
import com.synload.nucleo.NucleoMesh;
import com.synload.nucleo.data.NucleoData;
import com.synload.nucleo.data.NucleoObject;
import com.synload.nucleo.event.*;
import org.reflections.Reflections;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Hub {

    private HashMap<String, NucleoResponder> responders = Maps.newHashMap();
    private String bootstrap;
    public static ObjectMapper objectMapper = new ObjectMapper();
    private String uniqueName;
    private NucleoMesh mesh;
    private boolean offline = true;
    private ExecutorService executorService = Executors.newWorkStealingPool();
    private TrafficHandler trafficHandler = new TrafficHandler();
    protected static final Logger logger = LoggerFactory.getLogger(Hub.class);


    public Hub(NucleoMesh mesh, String uniqueName) {
        this.uniqueName = uniqueName;
        this.mesh = mesh;
        this.offline = false;
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
                createOrUpdate("root", new NucleoData(data));
            }}), new NucleoResponder() {
                @Override
                public void run(NucleoData returnedData) {
                }
            }, false);
        }
    }

    public void nextChain(NucleoData data) {
        int onChain = data.getOnChain();
        if (data.getTrack() != 0)
            try {
                logger.debug("before: " + new ObjectMapper().writeValueAsString(data));
            } catch (Exception e) {
                e.printStackTrace();
            }
        List<NucleoData> datas = trafficHandler.getNext(data);
        if (data.getTrack() != 0) {
            try {
                //logger.debug("next: " + new ObjectMapper().writeValueAsString(datas));
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        if (datas != null) {
            logger.debug(data.getRoot().toString() + ": data parts " + datas.size());
            datas.stream().forEach(x -> {
                if (x.currentChain() == null) {
                    logger.debug(x.currentChainString() + " - sending to origin");
                    sendRoot(x);
                } else {
                    String topic = x.currentChainString();
                    if (onChain != x.getOnChain() && onChain > -1 && data.getChainList().get(onChain).getParallelChains().size() > 0) {
                        logger.debug(x.currentChainString() + ": sending to leader to re-assemble");
                        mesh.getInterlinkManager().leader(topic, x);
                    } else {
                        /*if (x.getChainList().get(x.getOnChain()).getParallelChains().size() > 0) {
                            logger.debug(x.currentChain() + ": routing to complete parallel");
                        } else {
                            logger.debug(x.currentChain() + ": routing to complete chain");
                        }*/
                        sendTopic(topic, x);
                    }
                }
            });
        }
    }

    public void sendTopic(String topic, NucleoData data) {
        logger.debug(data.getRoot().toString() + " - sending " + topic);
        mesh.getInterlinkManager().send(topic, data);
    }

    public void sendBroadcastTopic(String topic, NucleoData data) {
        logger.debug(data.getRoot().toString() + " - broadcasting to all services listening to " + topic);
        mesh.getInterlinkManager().broadcast(topic, data);
    }

    public void sendLeaderTopic(String topic, NucleoData data) {
        logger.debug(data.getRoot().toString() + " - broadcasting to all services listening to " + topic);
        mesh.getInterlinkManager().leader(topic, data);
    }

    public void sendRoot(NucleoData data) {
        logger.debug(data.getRoot().toString() + " - sending back to origin");
        mesh.getInterlinkManager().send("nucleo.client." + data.getOrigin(), data);
    }

    public void push(NucleoData data, NucleoResponder responder, boolean allowTracking) {
        if (offline) {
            logger.error("Attempting to use a closed hub!");
            return;
        }
        responders.put(data.getRoot().toString(), responder);
        nextChain(data);
    }

    public void register(String... servicePackages) {
        Arrays.stream(servicePackages).forEach(servicePackage -> {
            try {
                Reflections reflect = new Reflections(servicePackage);
                Set<Class<?>> classes = reflect.getTypesAnnotatedWith(NucleoClass.class);
                mesh.getEventHandler().registerClasses(classes);
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
    }

    public void handle(Hub hub, NucleoData data) {
        String topic = data.currentChainString();
        executorService.submit(() -> trafficHandler.processParallel(data, new TrafficExecutor(hub, data, topic)));
    }

    public void close() {
        offline = true;
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
