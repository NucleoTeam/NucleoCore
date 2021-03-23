package com.synload.nucleo.zookeeper;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.synload.nucleo.NucleoMesh;
import org.apache.curator.x.discovery.ServiceDiscovery;
import org.apache.curator.x.discovery.ServiceDiscoveryBuilder;
import org.apache.curator.x.discovery.ServiceInstance;
import org.apache.curator.x.discovery.UriSpec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;

public class ZooKeeperServiceRegistration {
    protected static final Logger logger = LoggerFactory.getLogger(ZooKeeperServiceRegistration.class);

    private ServiceDiscovery<byte[]> serviceDiscovery = null;

    @JsonIgnore
    private NucleoMesh mesh;
    private String host;

    public ZooKeeperServiceRegistration(NucleoMesh mesh, ServiceDiscovery<byte[]> serviceDiscovery, String host){
        this.serviceDiscovery = serviceDiscovery;
        this.mesh = mesh;
        this.host = host;
    }
    public void registerService(){
        try {
            ServiceInstance<byte[]> thisInstance = ServiceInstance.<byte[]>builder()
                .name(mesh.getServiceName())
                .id(mesh.getUniqueName())
                .payload(new ObjectSerializer().serialize(new ServiceInformation(
                    mesh.getMeshName(),
                    mesh.getServiceName(),
                    mesh.getUniqueName(),
                    new ArrayList<>(mesh.getChainHandler().getLinks().values()),
                    false
                )))
                .uriSpec(new UriSpec(host))
                .build();
            serviceDiscovery.registerService(thisInstance);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    public void unregister(){
        try {
            ServiceInstance<byte[]> thisInstance = ServiceInstance.<byte[]>builder()
                .name(mesh.getServiceName())
                .id(mesh.getUniqueName())
                .payload(new ObjectSerializer().serialize(new ServiceInformation(
                    mesh.getMeshName(),
                    mesh.getServiceName(),
                    mesh.getUniqueName(),
                    new ArrayList<>(mesh.getChainHandler().getLinks().values()),
                    false
                )))
                .uriSpec(new UriSpec(host))
                .build();
            serviceDiscovery.unregisterService(thisInstance);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
