package com.synload.nucleo.zookeeper;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.synload.nucleo.NucleoMesh;
import org.apache.curator.x.discovery.ServiceDiscovery;
import org.apache.curator.x.discovery.ServiceDiscoveryBuilder;
import org.apache.curator.x.discovery.ServiceInstance;
import org.apache.curator.x.discovery.UriSpec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ZooKeeperServiceRegistration {
    protected static final Logger logger = LoggerFactory.getLogger(ZooKeeperServiceRegistration.class);

    private ServiceDiscovery<ServiceInformation> serviceDiscovery = null;

    @JsonIgnore
    private NucleoMesh mesh;
    private String host;

    public ZooKeeperServiceRegistration(NucleoMesh mesh, ServiceDiscovery<ServiceInformation> serviceDiscovery, String host){
        this.serviceDiscovery = serviceDiscovery;
        this.mesh = mesh;
        this.host = host;
    }
    public void registerService(){
        try {
            ServiceInstance<ServiceInformation> thisInstance = ServiceInstance.<ServiceInformation>builder()
                .name(mesh.getServiceName())
                .id(mesh.getUniqueName())
                .payload(new ServiceInformation(
                    mesh.getMeshName(),
                    mesh.getServiceName(),
                    mesh.getUniqueName(),
                    mesh.getChainHandler().getChainToMethod().values(),
                    false
                ))
                .uriSpec(new UriSpec(host))
                .build();
            serviceDiscovery.registerService(thisInstance);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    public void unregister(){
        try {
            ServiceInstance<ServiceInformation> thisInstance = ServiceInstance.<ServiceInformation>builder()
                .name(mesh.getServiceName())
                .id(mesh.getUniqueName())
                .payload(new ServiceInformation(
                    mesh.getMeshName(),
                    mesh.getServiceName(),
                    mesh.getUniqueName(),
                    mesh.getChainHandler().getChainToMethod().values(),
                    false
                ))
                .uriSpec(new UriSpec(host))
                .build();
            serviceDiscovery.unregisterService(thisInstance);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
