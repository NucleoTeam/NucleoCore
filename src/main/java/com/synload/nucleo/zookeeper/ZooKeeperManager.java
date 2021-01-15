package com.synload.nucleo.zookeeper;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Maps;
import com.synload.nucleo.NucleoMesh;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.leader.LeaderSelector;
import org.apache.curator.framework.recipes.leader.LeaderSelectorListener;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.x.discovery.*;
import org.apache.curator.x.discovery.details.JsonInstanceSerializer;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetAddress;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class ZooKeeperManager {
    protected static final Logger logger = LoggerFactory.getLogger(ZooKeeperManager.class);

    private String connString;
    private NucleoMesh mesh;

    private static ServiceDiscovery<ServiceInformation> serviceDiscovery = null;
    private CuratorFramework client = null;

    private ZooKeeperLeadershipClient leadershipClient;
    private ZooKeeperServiceMonitor serviceMonitor;
    private ZooKeeperServiceRegistration serviceRegistration;



    public ZooKeeperManager(String zkConnectionString, NucleoMesh mesh) {
        try {
            this.connString = zkConnectionString;
            this.mesh = mesh;
        } catch (Exception e) {
            e.printStackTrace();
        }

    }


    public void create() {
        try {
            String host = InetAddress.getLocalHost().getHostAddress();
            String hostName = InetAddress.getLocalHost().getHostName();

            String path = "/discovery/" + mesh.getMeshName();
            String leaderPath = "/leader/" + mesh.getMeshName();
            logger.info("Registering this service to zookeeper");
            logger.info("============Service Debug==========");
            logger.info("Zookeeper: " + connString);
            logger.info("Path: " + path);
            logger.info("Unique Id: " + mesh.getUniqueName());
            logger.info("Service Name: " + mesh.getServiceName());
            logger.info("IP Address: " + host);
            logger.info("Port: " + mesh.getInterlinkManager().getPort());
            logger.info("HostName: " + hostName);

            client = CuratorFrameworkFactory.newClient(connString, new ExponentialBackoffRetry(500, 2));
            client.start();
            client.blockUntilConnected();

            JsonInstanceSerializer<ServiceInformation> serializer = new JsonInstanceSerializer<ServiceInformation>(ServiceInformation.class);

            serviceDiscovery = ServiceDiscoveryBuilder.builder(ServiceInformation.class)
                .client(client)
                .basePath(path)
                .serializer(serializer)
                .build();
            serviceDiscovery.start();

            logger.info("Connected to ZooKeeper");

            leadershipClient = new ZooKeeperLeadershipClient(mesh, client, serviceDiscovery, leaderPath, host, hostName);
            serviceMonitor = new ZooKeeperServiceMonitor(mesh, serviceDiscovery);
            serviceRegistration = new ZooKeeperServiceRegistration(mesh, serviceDiscovery, host, hostName);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    Thread serviceMonitorThread;
    public void start(){
        serviceRegistration.registerService();
        leadershipClient.start();
        serviceMonitorThread = new Thread(serviceMonitor);
        serviceMonitorThread.start();
    }


    public void close() throws IOException {
        leadershipClient.close();
        serviceMonitorThread.interrupt();
        serviceRegistration.unregister();
        serviceDiscovery.close();
        client.close();
    }

    public CuratorFramework getClient() {
        return client;
    }

    public void setClient(CuratorFramework client) {
        this.client = client;
    }
}
