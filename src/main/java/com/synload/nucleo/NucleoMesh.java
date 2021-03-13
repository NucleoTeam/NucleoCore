package com.synload.nucleo;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.synload.nucleo.chain.ChainHandler;
import com.synload.nucleo.chain.path.PathBuilder;
import com.synload.nucleo.chain.path.Run;
import com.synload.nucleo.data.NucleoData;
import com.synload.nucleo.data.NucleoObject;
import com.synload.nucleo.event.ClassScanner;
import com.synload.nucleo.event.EventHandler;
import com.synload.nucleo.event.NucleoClass;
import com.synload.nucleo.event.NucleoResponder;
import com.synload.nucleo.hub.Hub;
import com.synload.nucleo.interlink.InterlinkEventType;
import com.synload.nucleo.interlink.InterlinkManager;
import com.synload.nucleo.utils.NucleoDataStats;
import com.synload.nucleo.zookeeper.ZooKeeperManager;
import org.apache.curator.shaded.com.google.common.collect.Lists;
import org.reflections.Reflections;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Serializable;
import java.util.*;


public class NucleoMesh {
    private Hub hub;
    private String uniqueName;
    private ZooKeeperManager manager;
    private String meshName;
    private String serviceName;
    private InterlinkManager interlinkManager;
    private EventHandler eventHandler = new EventHandler();
    private ChainHandler chainHandler = new ChainHandler();
    protected static final Logger logger = LoggerFactory.getLogger(NucleoMesh.class);


    public NucleoMesh(String meshName, String serviceName, String zookeeper, String kafkaServers, String... packageStr) throws ClassNotFoundException {
        this.uniqueName = UUID.randomUUID().toString();
        hub = new Hub(this);
        this.meshName = meshName;
        this.serviceName = serviceName;
        logger.info("Starting nucleo client and joining mesh " + meshName + " with service name " + serviceName);

        interlinkManager = new InterlinkManager(this, kafkaServers);


        logger.info("Registering event methods.");
        registerPackage("com.synload.nucleo.interlink.handlers");
        registerPackage(packageStr);

        getEventHandler().callInterlinkEvent(InterlinkEventType.RECEIVE_TOPIC, this, "test");

        interlinkManager.subscribe(getChainHandler().getLinks().keySet());
        interlinkManager.subscribe("nucleo.client."+this.uniqueName);
        interlinkManager.subscribeBroadcasts(getChainHandler().getLinks().keySet());

        interlinkManager.start();
        try {
            manager = new ZooKeeperManager(zookeeper, this);
            manager.create();
            manager.start();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void registerPackage(String... servicePackages) {
        Arrays.stream(servicePackages).forEach(servicePackage -> {
            try {
                Reflections reflect = new Reflections(servicePackage);
                Set<Class<?>> classes = reflect.getTypesAnnotatedWith(NucleoClass.class);
                new ClassScanner(getEventHandler(), getChainHandler()).registerClasses(classes);
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
    }

    public void shutdown() throws IOException {
        // close down all connections / threads
        manager.close();
        interlinkManager.close();
        getHub().close();
    }


    public boolean call(Run chain, NucleoObject objects, NucleoResponder nucleoResponder) {

        // add origin to end of execution
        Run origin = PathBuilder.generateExactRun("nucleo.client."+this.getUniqueName()).getRoot();

        // handle the prequery optimizations here.
        Set<Run> lastSet = chain.last();
        origin.getParents().addAll(lastSet);
        lastSet.forEach(l->l.getNextRuns().add(origin));

        this.getHub().start(hub.constructNucleoData(chain, objects), nucleoResponder);
        return true;
    }

    public Hub getHub() {
        return hub;
    }

    public String getUniqueName() {
        return uniqueName;
    }

    public String getMeshName() {
        return meshName;
    }

    public String getServiceName() {
        return serviceName;
    }

    public InterlinkManager getInterlinkManager() {
        return interlinkManager;
    }

    public ZooKeeperManager getManager() {
        return manager;
    }

    public EventHandler getEventHandler() {
        return eventHandler;
    }

    public ChainHandler getChainHandler() {
        return chainHandler;
    }

    public void setChainHandler(ChainHandler chainHandler) {
        this.chainHandler = chainHandler;
    }
}
