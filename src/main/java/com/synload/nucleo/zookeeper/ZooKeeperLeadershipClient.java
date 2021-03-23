package com.synload.nucleo.zookeeper;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.synload.nucleo.NucleoMesh;
import com.synload.nucleo.interlink.InterlinkEventType;
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
import java.util.ArrayList;
import java.util.concurrent.CountDownLatch;

public class ZooKeeperLeadershipClient implements Closeable, LeaderSelectorListener {
    protected static final Logger logger = LoggerFactory.getLogger(ZooKeeperLeadershipClient.class);

    private CountDownLatch countDownLatch = new CountDownLatch(1);
    private ServiceDiscovery<byte[]> serviceDiscovery = null;

    private String serviceName = "";
    private LeaderSelector leader = null;
    private NucleoMesh mesh;
    private String topicName;

    public ZooKeeperLeadershipClient(NucleoMesh mesh, CuratorFramework client, ServiceDiscovery<byte[]> serviceDiscovery, String leaderPath, String topicName) {
        this.mesh = mesh;
        this.serviceDiscovery = serviceDiscovery;
        serviceName = mesh.getHub().getMesh().getServiceName();
        logger.info(leaderPath + "/for/" + topicName);
        this.topicName=topicName;
        leader = new LeaderSelector(
            client,
            leaderPath + "/for/" + topicName,
            this
        );
        leader.setId(mesh.getUniqueName());
        leader.autoRequeue();
    }

    public void start() {
        leader.start();
    }

    @Override
    public void takeLeadership(CuratorFramework client) throws Exception {
        ServiceInformation serviceInformation = new ServiceInformation(
            mesh.getMeshName(),
            mesh.getServiceName(),
            mesh.getUniqueName(),
            new ArrayList<>(mesh.getChainHandler().getLinks().values()),
            true
        );
        ServiceInstance<byte[]> thisInstance = ServiceInstance.<byte[]>builder()
            .id(mesh.getUniqueName())
            .name(mesh.getServiceName())
            .payload(new ObjectSerializer().serialize(serviceInformation))
            .build();
        serviceDiscovery.updateService(thisInstance);
        logger.info("New leader for " + topicName + ", "+serviceName+" ( " + mesh.getUniqueName() + " ) is the new leader");
        mesh.getEventHandler().callInterlinkEvent(InterlinkEventType.GAIN_LEADER, mesh, serviceInformation, topicName);
        try {
            countDownLatch.await();
            countDownLatch = new CountDownLatch(1);
        } catch (Exception e) {
            e.printStackTrace();
        }
        mesh.getEventHandler().callInterlinkEvent(InterlinkEventType.CEDE_LEADER, mesh, serviceInformation, topicName);
        logger.info("This service gave up leadership of the topic "+topicName);
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
                    ServiceInstance<byte[]> thisInstance = ServiceInstance.<byte[]>builder()
                        .id(mesh.getUniqueName())
                        .name(mesh.getServiceName())
                        .payload(new ObjectSerializer().serialize(new ServiceInformation(
                            mesh.getMeshName(),
                            mesh.getServiceName(),
                            mesh.getUniqueName(),
                            new ArrayList<>(mesh.getChainHandler().getLinks().values()),
                            false
                        )))
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
