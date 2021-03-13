package com.synload.nucleo.hub;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.*;
import com.synload.nucleo.NucleoMesh;
import com.synload.nucleo.chain.ChainExecution;
import com.synload.nucleo.chain.path.PathBuilder;
import com.synload.nucleo.chain.path.Run;
import com.synload.nucleo.chain.path.SingularRun;
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
    private NucleoMesh mesh;
    private boolean offline = true;
    private ExecutorService executorService = Executors.newWorkStealingPool();
    private TrafficHandler trafficHandler = new TrafficHandler();
    protected static final Logger logger = LoggerFactory.getLogger(Hub.class);


    public Hub(NucleoMesh mesh) {
        this.mesh = mesh;
        this.offline = false;
    }

    public NucleoData constructNucleoData(Run chain, NucleoObject objects) {
        logger.debug("Constructing request");
        NucleoData data = new NucleoData();
        data.setObjects(objects);
        data.setOrigin(mesh.getUniqueName());
        data.setChainExecution(new ChainExecution(chain));
        return data;
    }

    public void log(String state, NucleoData data) {
        if (data.getTrack() == 1) {
            data.setVersion(data.getVersion() + 1);
            start(constructNucleoData(PathBuilder.generateExactRun("_watch." + state).getRoot(), new NucleoObject() {{
                createOrUpdate("root", new NucleoData(data));
            }}), returnedData -> {});
        }
    }

    public void nextChain(NucleoData data) {
        List<NucleoData> dataList = trafficHandler.getNext(data);
        if (dataList != null) {
            logger.info(data.getRoot().toString() + ": data parts " + dataList.size());
            dataList.stream().forEach(nucleoData -> {
                String topic = nucleoData.currentChainString();
                long previousParallelCount = nucleoData.getChainExecution().getCurrent().getParents().stream().filter(f->f.isParallel()).count();
                if (!nucleoData.getChainExecution().getCurrent().isParallel() && previousParallelCount > 0 && !topic.startsWith("nucleo.client")) {
                    logger.info(nucleoData.currentChainString() + ": sending to leader to re-assemble");
                    mesh.getInterlinkManager().leader(topic, nucleoData);
                } else {
                    sendTopic(topic, nucleoData);
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

    public void start(NucleoData data, NucleoResponder responder) {
        if (offline) {
            logger.error("Attempting to use a closed hub!");
            return;
        }
        responders.put(data.getRoot().toString(), responder);
        trafficHandler.current(data).forEach(nucleoData->sendTopic(((SingularRun)nucleoData.getChainExecution().getCurrent()).getChain(), nucleoData));;
    }

    public void handle(Hub hub, NucleoData data) {
        String topic = data.currentChainString();
        executorService.submit(() -> trafficHandler.process(data, new TrafficExecutor(hub, data, topic)));
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
