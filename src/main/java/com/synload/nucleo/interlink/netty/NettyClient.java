package com.synload.nucleo.interlink.netty;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.synload.nucleo.NucleoMesh;
import com.synload.nucleo.data.NucleoData;
import com.synload.nucleo.interlink.InterlinkMessage;
import com.synload.nucleo.zookeeper.ServiceInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.net.DatagramSocket;
import java.net.InetAddress;

public class NettyClient {
    @JsonIgnore
    protected static final Logger logger = LoggerFactory.getLogger(NettyClient.class);

    public ServiceInformation node;

    @JsonIgnore
    public NucleoMesh mesh;

    private NettyDatagramUtils utils;

    @JsonIgnore
    private static ObjectMapper mapper = new ObjectMapper(){{
        this.enableDefaultTyping();
        this.setSerializationInclusion(JsonInclude.Include.NON_NULL);
    }};

    public NettyClient(ServiceInformation node, NucleoMesh mesh){
        this.node = node;
        this.mesh = mesh;
        this.utils = new NettyDatagramUtils();
    }

    public void add(String topic, NucleoData data){
        try {
            DatagramSocket socket = new DatagramSocket();
            String[] connectionInfo = node.getConnectString().split(":");
            InetAddress address = InetAddress.getByName(connectionInfo[0]);
            try {
                utils.send(socket, new InterlinkMessage(topic, data), address, Integer.valueOf(connectionInfo[1]));
            }catch (Exception e){
                e.printStackTrace();
            }
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    public ServiceInformation getNode() {
        return node;
    }

    public void setNode(ServiceInformation node) {
        this.node = node;
    }

    public NucleoMesh getMesh() {
        return mesh;
    }

    public void setMesh(NucleoMesh mesh) {
        this.mesh = mesh;
    }

    public NettyDatagramUtils getUtils() {
        return utils;
    }

    public void setUtils(NettyDatagramUtils utils) {
        this.utils = utils;
    }

}
