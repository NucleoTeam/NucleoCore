package com.synload.nucleo.zookeeper;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Collections2;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.synload.nucleo.NucleoMesh;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.BackgroundCallback;
import org.apache.curator.framework.api.CuratorEvent;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.utils.CloseableUtils;
import org.apache.curator.x.discovery.*;
import org.apache.curator.x.discovery.details.JsonInstanceSerializer;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;

import java.net.InetAddress;
import java.util.*;
import java.util.concurrent.TimeUnit;

public class ZooKeeperManager implements Runnable {

    private CuratorFramework zooClient;
    private Connection connection;
    private String connString;
    private String meshName;
    private NucleoMesh mesh;
    private ServiceDiscovery<ServiceInformation> serviceDiscovery = null;
    private ServiceDiscovery<ServiceInformation> serviceDiscoveryInformation = null;
    private CuratorFramework client = null;

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

    public String getConnString() {
        return connString;
    }

    public void setConnString(String connString) {
        this.connString = connString;
    }

    public void registerServiceWithZKeeper() {
        System.out.println("Registering this service to zookeeper");
        System.out.println("============Service Debug==========");
        System.out.println("Unique Id: " + mesh.getUniqueName());
        System.out.println("Service Name: " + mesh.getServiceName());
        try {
            String address = InetAddress.getLocalHost().getHostAddress();
            System.out.println("IP Address: " + address);
            System.out.println("Port: " + mesh.geteManager().getPort());
            String hostName = InetAddress.getLocalHost().getHostName();
            register(
                "/" + meshName + "/services/" + mesh.getServiceName() + "/" + mesh.getUniqueName(),
                new ObjectMapper().writeValueAsBytes(new ServiceInformation(
                    meshName,
                    mesh.getServiceName(),
                    mesh.getUniqueName(),
                    mesh.getHub().getEventHandler().getChainToMethod().keySet(),
                    address + ":" + mesh.geteManager().getPort(),
                    hostName
                ))
            );
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void run() {
        ServiceInstance<ServiceInformation> thisInstance = null;
        Map<String, ServiceProvider<ServiceInformation>> providers = Maps.newHashMap();
        try {
            String host = InetAddress.getLocalHost().getHostAddress();
            String hostName = InetAddress.getLocalHost().getHostName();
            String path = "/discovery/" + mesh.getMeshName();
            System.out.println("Registering this service to zookeeper");
            System.out.println("============Service Debug==========");
            System.out.println("Zookeeper: " + connString);
            System.out.println("Path: " + path);
            System.out.println("Unique Id: " + mesh.getUniqueName());
            System.out.println("Service Name: " + mesh.getServiceName());
            System.out.println("IP Address: " + host);
            System.out.println("Port: " + mesh.geteManager().getPort());
            System.out.println("HostName: " + hostName);

            client = CuratorFrameworkFactory.newClient(this.connString, new ExponentialBackoffRetry(1000, 100));
            client.start();

            JsonInstanceSerializer<ServiceInformation> serializer = new JsonInstanceSerializer<ServiceInformation>(ServiceInformation.class);
            serviceDiscovery = ServiceDiscoveryBuilder.builder(ServiceInformation.class).client(client).basePath(path).serializer(serializer).build();
            serviceDiscovery.start();

            UriSpec uriSpec = new UriSpec(host + ":{port}");


            thisInstance = ServiceInstance.<ServiceInformation>builder()
                .name(mesh.getServiceName())
                .payload(new ServiceInformation(
                    mesh.getMeshName(),
                    mesh.getServiceName(),
                    mesh.getUniqueName(),
                    mesh.getHub().getEventHandler().getChainToMethod().keySet(),
                    host + ":" + mesh.geteManager().getPort(),
                    hostName
                ))
                .port(mesh.geteManager().getPort()) // in a real application, you'd use a common port
                .uriSpec(uriSpec)
                .build();

            serviceDiscoveryInformation = ServiceDiscoveryBuilder.builder(ServiceInformation.class)
                .client(client)
                .basePath(path)
                .serializer(serializer)
                .thisInstance(thisInstance)
                .build();
            serviceDiscoveryInformation.start();

        } catch (Exception e) {
            e.printStackTrace();
        }
        new Thread(new WatchNodeList()).start();
    }

    public void delete(String path, BackgroundCallback callback) {
        try {
            zooClient.delete().inBackground(callback).forPath(path);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void create(String path, BackgroundCallback callback) {
        try {
            zooClient.create().withMode(CreateMode.PERSISTENT).inBackground(callback).forPath(path);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void closeConnection() throws Exception {
        connection.close();
    }

    public void outputInstance(ServiceInstance<ServiceInformation> instance) {
        System.out.println("\t" + instance.getPayload().getService() + ": " + instance.buildUriSpec());
        System.out.println("\t Events: " + instance.getPayload().getEvents().size());
    }

    public void listInstances() throws Exception {
        // This shows how to query all the instances in service discovery

        try {
            Collection<String> serviceNames = serviceDiscovery.queryForNames();
            System.out.println(serviceNames.size() + " type(s)");
            for (String serviceName : serviceNames) {

                Collection<ServiceInstance<ServiceInformation>> instances = serviceDiscovery.queryForInstances(serviceName);
                for (ServiceInstance<ServiceInformation> instance : instances) {
                    outputInstance(instance);
                }
            }
        } finally {
            CloseableUtils.closeQuietly(serviceDiscovery);
        }
    }


    public void register(String path, byte[] data) {
        try {

            zooClient.create().withMode(CreateMode.EPHEMERAL).inBackground((CuratorFramework client, CuratorEvent event) -> {
                System.out.println("=======================================================");
                System.out.println(path);
                System.out.println(KeeperException.Code.get(event.getResultCode()));
            }).forPath(path, data);

        } catch (Exception e) {
            e.printStackTrace();
        }
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
                                if (connectedList.contains(instance.getPayload().getName())) {
                                    // disregard
                                }
                                if (!connectedList.contains(instance)) {
                                    mesh.geteManager().sync(instance.getPayload());
                                    connectedList.add(instance.getPayload().getName());
                                }
                            }
                            List<String> newTMPConnected = new ArrayList<String>(connectedList);
                            for (String instance : newTMPConnected) {
                                if (instances.stream().filter(x->x.getPayload().getName().equals(instance)).count()==0){
                                    mesh.geteManager().delete(instance);
                                }
                            }
                        } else {
                            List<String> instancesString = new ArrayList<>();
                            for (ServiceInstance<ServiceInformation> instance : instances) {
                                mesh.geteManager().sync(instance.getPayload());
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

    public ServiceDiscovery<ServiceInformation> getServiceDiscoveryInformation() {
        return serviceDiscoveryInformation;
    }

    public void setServiceDiscoveryInformation(ServiceDiscovery<ServiceInformation> serviceDiscoveryInformation) {
        this.serviceDiscoveryInformation = serviceDiscoveryInformation;
    }

    public CuratorFramework getClient() {
        return client;
    }

    public void setClient(CuratorFramework client) {
        this.client = client;
    }
}
