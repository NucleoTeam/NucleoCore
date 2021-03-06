package com.synload.nucleo;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.synload.nucleo.data.NucleoData;
import com.synload.nucleo.data.NucleoObject;
import com.synload.nucleo.event.EventHandler;
import com.synload.nucleo.event.NucleoResponder;
import com.synload.nucleo.hub.Hub;
import com.synload.nucleo.interlink.InterlinkManager;
import com.synload.nucleo.utils.NucleoDataStats;
import com.synload.nucleo.zookeeper.ZooKeeperManager;
import org.apache.curator.shaded.com.google.common.collect.Lists;
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
    protected static final Logger logger = LoggerFactory.getLogger(NucleoMesh.class);


    public NucleoMesh(String meshName, String serviceName, String zookeeper, String kafkaServers, String... packageStr) throws ClassNotFoundException {
        this.uniqueName = UUID.randomUUID().toString();
        hub = new Hub(this, this.uniqueName);
        this.meshName = meshName;
        this.serviceName = serviceName;
        logger.info("Starting nucleo client and joining mesh " + meshName + " with service name " + serviceName);

        interlinkManager = new InterlinkManager(this, kafkaServers);
        logger.info("Registering event methods.");
        getHub().register("com.synload.nucleo.interlink.handlers");
        getHub().register(packageStr);
        interlinkManager.subscribe(getEventHandler().getChainToMethod().keySet());
        interlinkManager.subscribe("nucleo.client."+this.uniqueName);
        interlinkManager.subscribeBroadcasts(getEventHandler().getChainToMethod().keySet());

        interlinkManager.start();
        try {
            manager = new ZooKeeperManager(zookeeper, this);
            manager.create();
            manager.start();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void shutdown() throws IOException {
        // close down all connections / threads
        manager.close();
        interlinkManager.close();
        getHub().close();
    }


    public void call(String chain, NucleoObject objects, NucleoResponder nucleoResponder) {
        this.getHub().push(hub.constructNucleoData(chain, objects), nucleoResponder, true);
    }

    public boolean call(String[] chains, NucleoObject objects, NucleoResponder nucleoResponder) {
        if (chains.length == 0) {
            return false;
        }
        this.getHub().push(hub.constructNucleoData(chains, objects), nucleoResponder, true);
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
}
