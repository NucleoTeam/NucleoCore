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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.util.*;
import java.util.concurrent.CountDownLatch;

public class ZooKeeperManager implements Runnable {

    private CuratorFramework zooClient;
    private Connection connection;
    private String connString;
    private String meshName;
    private NucleoMesh mesh;
    private static ServiceDiscovery<ServiceInformation> serviceDiscovery = null;
    private CuratorFramework client = null;
    private static LeaderSelector leader = null;
    private CountDownLatch latch = null;
    protected static final Logger logger = LoggerFactory.getLogger(ZooKeeperManager.class);

    public ZooKeeperManager(String zkConnectionString, NucleoMesh mesh) {
        try {
            this.connString = zkConnectionString;
            this.meshName = mesh.getMeshName();
            this.mesh = mesh;
            connection = new Connection();
            new Thread(this).start();

        } catch (Exception e) {
            e.printStackTrace();
        }

    }


    public void run() {

        Map<String, ServiceProvider<ServiceInformation>> providers = Maps.newHashMap();
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

            client = CuratorFrameworkFactory.newClient(this.connString, new ExponentialBackoffRetry(1000, 5));
            client.start();

            JsonInstanceSerializer<ServiceInformation> serializer = new JsonInstanceSerializer<ServiceInformation>(ServiceInformation.class);

            UriSpec uriSpec = new UriSpec(host + ":{port}");

            serviceDiscovery = ServiceDiscoveryBuilder.builder(ServiceInformation.class)
                .client(client)
                .basePath(path)
                .serializer(serializer)
                .build();
            serviceDiscovery.start();

            logger.info("CONNECTED");
            try {
                if (latch != null)
                    latch.countDown();
                ServiceInstance<ServiceInformation> thisInstance = ServiceInstance.<ServiceInformation>builder()
                    .name(mesh.getServiceName())
                    .id(mesh.getUniqueName())
                    .payload(new ServiceInformation(
                        mesh.getMeshName(),
                        mesh.getServiceName(),
                        mesh.getUniqueName(),
                        mesh.getHub().getEventHandler().getChainToMethod().keySet(),
                        host + ":" + mesh.getInterlinkManager().getPort(),
                        hostName,
                        false
                    ))
                    .port(mesh.getInterlinkManager().getPort()) // in a real application, you'd use a common port
                    .uriSpec(uriSpec)
                    .build();
                serviceDiscovery.registerService(thisInstance);
            } catch (Exception e) {
                e.printStackTrace();
            }

            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                try {
                    if (leader.hasLeadership()) {
                        leader.requeue();
                    }
                    leader.close();
                    serviceDiscovery.close();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }));
            String serviceName = mesh.getHub().getMesh().getServiceName();

            logger.info(leaderPath + "/for/" + serviceName);
            leader = new LeaderSelector(
                client,
                leaderPath + "/for/" + serviceName,
                new LeaderSelectorListener() {
                    @Override
                    public void stateChanged(CuratorFramework client, ConnectionState newState) {
                        switch (newState) {
                            case LOST:
                            case SUSPENDED:
                                if (latch != null)
                                    latch.countDown();
                                break;
                            case RECONNECTED:
                                try {
                                    if (latch != null)
                                        latch.countDown();
                                    ServiceInstance<ServiceInformation> thisInstance = ServiceInstance.<ServiceInformation>builder()
                                        .id(mesh.getUniqueName())
                                        .name(mesh.getServiceName())
                                        .payload(new ServiceInformation(
                                            mesh.getMeshName(),
                                            mesh.getServiceName(),
                                            mesh.getUniqueName(),
                                            mesh.getHub().getEventHandler().getChainToMethod().keySet(),
                                            host + ":" + mesh.getInterlinkManager().getPort(),
                                            hostName,
                                            false
                                        ))
                                        .port(mesh.getInterlinkManager().getPort())
                                        .uriSpec(uriSpec)
                                        .build();
                                    serviceDiscovery.registerService(thisInstance);
                                } catch (Exception e) {
                                    e.printStackTrace();
                                }
                                break;
                            case CONNECTED:

                                break;
                        }
                        try {
                            logger.debug(new ObjectMapper().writeValueAsString(newState));
                            logger.debug("STATE CHANGED");
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }

                    @Override
                    public void takeLeadership(CuratorFramework client) throws Exception {
                        try {
                            logger.info("New leader for " + serviceName + ", " + mesh.getUniqueName() + " is the new leader");
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                        ServiceInstance<ServiceInformation> thisInstance = ServiceInstance.<ServiceInformation>builder()
                            .id(mesh.getUniqueName())
                            .name(mesh.getServiceName())
                            .payload(new ServiceInformation(
                                mesh.getMeshName(),
                                mesh.getServiceName(),
                                mesh.getUniqueName(),
                                mesh.getHub().getEventHandler().getChainToMethod().keySet(),
                                host + ":" + mesh.getInterlinkManager().getPort(),
                                hostName,
                                true
                            ))
                            .port(mesh.getInterlinkManager().getPort())
                            .uriSpec(uriSpec)
                            .build();
                        serviceDiscovery.updateService(thisInstance);
                        latch = new CountDownLatch(1);
                        latch.await();
                    }
                }
            );
            leader.setId(mesh.getUniqueName());
            leader.autoRequeue();
            leader.start();
        } catch (Exception e) {
            e.printStackTrace();
        }
        new Thread(new WatchNodeList()).start();
    }


    HashMap<String, List<String>> connected = new HashMap<>();

    public class WatchNodeList implements Runnable {
        public void run() {
            ObjectMapper om = new ObjectMapper();
            while (true) {
                try {
                    Collection<String> serviceNames = serviceDiscovery.queryForNames();
                    for (String serviceName : serviceNames) {
                        Collection<ServiceInstance<ServiceInformation>> instances = serviceDiscovery.queryForInstances(serviceName);
                        if (connected.containsKey(serviceName)) {
                            List<String> connectedList = connected.get(serviceName);
                            for (ServiceInstance<ServiceInformation> instance : instances) {
                                mesh.getInterlinkManager().leaderCheck(instance.getPayload());
                                if (connectedList.contains(instance.getPayload().getName())) {
                                    // disregard
                                }
                                if (!connectedList.contains(instance)) {
                                    mesh.getInterlinkManager().sync(instance.getPayload());
                                    connectedList.add(instance.getPayload().getName());
                                }
                            }
                            List<String> newTMPConnected = new ArrayList<String>(connectedList);
                            for (String instance : newTMPConnected) {
                                if (instances.stream().filter(x -> x.getPayload().getName().equals(instance)).count() == 0) {
                                    mesh.getInterlinkManager().delete(instance);
                                    connectedList.remove(instance);
                                }
                            }
                        } else {
                            List<String> instancesString = new ArrayList<>();
                            for (ServiceInstance<ServiceInformation> instance : instances) {
                                mesh.getInterlinkManager().sync(instance.getPayload());
                                instancesString.add(instance.getPayload().getName());
                            }
                            connected.put(serviceName, instancesString);
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
                try {
                    Thread.sleep(1000);
                } catch (Exception e) {

                }
            }
        }
    }

    public CuratorFramework getZooClient() {
        return zooClient;
    }

    public void setZooClient(CuratorFramework zooClient) {
        this.zooClient = zooClient;
    }

    public ServiceDiscovery<ServiceInformation> getServiceDiscovery() {
        return serviceDiscovery;
    }

    public void setServiceDiscovery(ServiceDiscovery<ServiceInformation> serviceDiscovery) {
        this.serviceDiscovery = serviceDiscovery;
    }

    public CuratorFramework getClient() {
        return client;
    }

    public void setClient(CuratorFramework client) {
        this.client = client;
    }
}
