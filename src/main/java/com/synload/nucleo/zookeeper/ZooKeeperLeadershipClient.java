package com.synload.nucleo.zookeeper;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.synload.nucleo.NucleoMesh;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.leader.LeaderSelector;
import org.apache.curator.framework.recipes.leader.LeaderSelectorListener;
import org.apache.curator.framework.recipes.leader.LeaderSelectorListenerAdapter;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.x.discovery.ServiceDiscovery;
import org.apache.curator.x.discovery.ServiceInstance;
import org.apache.curator.x.discovery.UriSpec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.CountDownLatch;

public class ZooKeeperLeadershipClient implements Closeable, LeaderSelectorListener {
    protected static final Logger logger = LoggerFactory.getLogger(ZooKeeperLeadershipClient.class);

    private CountDownLatch countDownLatch = new CountDownLatch(1);
    private ServiceDiscovery<ServiceInformation> serviceDiscovery = null;

    private String serviceName = "";
    private LeaderSelector leader = null;
    private NucleoMesh mesh;
    private String host;
    private String hostName;

    public ZooKeeperLeadershipClient(NucleoMesh mesh, CuratorFramework client, ServiceDiscovery<ServiceInformation> serviceDiscovery, String leaderPath, String host, String hostName){
        this.mesh = mesh;
        this.host = host;
        this.hostName = hostName;
        this.serviceDiscovery = serviceDiscovery;
        serviceName = mesh.getHub().getMesh().getServiceName();
        logger.info(leaderPath + "/for/" + serviceName);
        leader = new LeaderSelector(
            client,
            leaderPath + "/for/" + serviceName,
            this
        );
        leader.setId(mesh.getUniqueName());
        leader.autoRequeue();
    }

    public void start(){
        leader.start();
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
            .uriSpec(new UriSpec(host + ":{port}"))
            .build();
        serviceDiscovery.updateService(thisInstance);
        logger.info("This service is now the leader of the mesh.");

        try{
            countDownLatch.await();
            countDownLatch = new CountDownLatch(1);
        }catch (Exception e){
            e.printStackTrace();
        }
        logger.info("This service gave up leadership.");
    }

    @Override
    public void stateChanged(CuratorFramework client, ConnectionState newState) {
        switch (newState) {
            case LOST:
            case SUSPENDED:
                countDownLatch.countDown();
                break;
            case RECONNECTED:
                try {
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
                        .uriSpec(new UriSpec(host + ":{port}"))
                        .build();
                    serviceDiscovery.registerService(thisInstance);
                } catch (Exception e) {
                    e.printStackTrace();
                }
                break;
            case CONNECTED:
                countDownLatch.countDown();
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
    public void close() {
        countDownLatch.countDown();
        leader.close();
    }
}
