package com.synload.nucleo.zookeeper;

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

    private NucleoMesh mesh;
    private String host;
    private String hostName;

    public ZooKeeperServiceRegistration(NucleoMesh mesh, ServiceDiscovery<ServiceInformation> serviceDiscovery, String host, String hostName){
        this.serviceDiscovery = serviceDiscovery;
        this.mesh = mesh;
        this.host = host;
        this.hostName = hostName;
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
                    mesh.getHub().getEventHandler().getChainToMethod().keySet(),
                    host + ":" + mesh.getInterlinkManager().getPort(),
                    hostName,
                    false
                ))
                .port(mesh.getInterlinkManager().getPort()) // in a real application, you'd use a common port
                .uriSpec(new UriSpec(host + ":{port}"))
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
                    mesh.getHub().getEventHandler().getChainToMethod().keySet(),
                    host + ":" + mesh.getInterlinkManager().getPort(),
                    hostName,
                    false
                ))
                .port(mesh.getInterlinkManager().getPort()) // in a real application, you'd use a common port
                .uriSpec(new UriSpec(host + ":{port}"))
                .build();
            serviceDiscovery.unregisterService(thisInstance);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
