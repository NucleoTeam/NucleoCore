package com.synload.nucleo.zookeeper;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.synload.nucleo.NucleoMesh;
import org.apache.curator.x.discovery.ServiceDiscovery;
import org.apache.curator.x.discovery.ServiceInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;

public class ZooKeeperServiceMonitor implements Runnable {
    protected static final Logger logger = LoggerFactory.getLogger(ZooKeeperServiceMonitor.class);

    ServiceDiscovery<ServiceInformation> serviceDiscovery;
    HashMap<String, List<String>> connected = new HashMap<>();
    NucleoMesh mesh;
    public ZooKeeperServiceMonitor(NucleoMesh mesh, ServiceDiscovery<ServiceInformation> serviceDiscovery){
        this.serviceDiscovery = serviceDiscovery;
        this.mesh = mesh;
    }
    public void run() {
        try {
            while (!Thread.currentThread().isInterrupted()) {
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
                Thread.sleep(5000);
            }
        } catch (Exception e) {

        }
    }
}
