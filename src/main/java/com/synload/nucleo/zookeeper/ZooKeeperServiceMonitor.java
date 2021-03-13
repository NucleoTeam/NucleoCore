package com.synload.nucleo.zookeeper;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.synload.nucleo.NucleoMesh;
import com.synload.nucleo.interlink.InterlinkEventType;
import org.apache.commons.lang3.concurrent.ConcurrentException;
import org.apache.curator.x.discovery.ServiceDiscovery;
import org.apache.curator.x.discovery.ServiceInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class ZooKeeperServiceMonitor implements Runnable {
    protected static final Logger logger = LoggerFactory.getLogger(ZooKeeperServiceMonitor.class);

    ServiceDiscovery<String> serviceDiscovery;
    List<ServiceInformation> clients = new LinkedList<>();
    NucleoMesh mesh;
    public ZooKeeperServiceMonitor(NucleoMesh mesh, ServiceDiscovery<String> serviceDiscovery){
        this.serviceDiscovery = serviceDiscovery;
        this.mesh = mesh;
    }
    public void run() {
        try {
            while (!Thread.currentThread().isInterrupted()) {
                try {
                    Collection<String> serviceNames = serviceDiscovery.queryForNames();
                    List<ServiceInformation> clientsPreCheck = new LinkedList<>(clients);
                    for (String serviceName : serviceNames) {
                        Collection<ServiceInstance<String>> instances = serviceDiscovery.queryForInstances(serviceName);
                        for (ServiceInstance<String> instance : instances) {
                            ServiceInformation serviceInformation = new ObjectSerializer().deserialize(instance.getPayload());
                            if (clients.stream().filter(c -> c.getUniqueName().equals(serviceInformation.getUniqueName())).count() == 0) {
                                clients.add(serviceInformation);
                                mesh.getEventHandler().callInterlinkEvent(InterlinkEventType.NEW_SERVICE, mesh, serviceInformation);
                            }else{
                                clientsPreCheck.removeIf(c->{
                                    String unique = c.getUniqueName();
                                    return c.getUniqueName().equals(unique);
                                });
                            }
                        }
                    }
                    clientsPreCheck.stream().forEach(serviceInformation->{
                        mesh.getEventHandler().callInterlinkEvent(InterlinkEventType.LEAVE_SERVICE, mesh, serviceInformation);
                    });
                } catch (Exception e) {
                    e.printStackTrace();
                }
                Thread.sleep(5000);
            }
        } catch (Exception e) {

        }
    }
}
