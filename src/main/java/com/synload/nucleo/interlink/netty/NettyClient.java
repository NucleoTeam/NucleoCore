package com.synload.nucleo.interlink.netty;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.synload.nucleo.NucleoMesh;
import com.synload.nucleo.data.NucleoData;
import com.synload.nucleo.interlink.InterlinkClient;
import com.synload.nucleo.interlink.InterlinkHandler;
import com.synload.nucleo.interlink.InterlinkMessage;
import com.synload.nucleo.zookeeper.ServiceInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.net.DatagramSocket;
import java.net.InetAddress;

public class NettyClient implements InterlinkClient {
    @JsonIgnore
    protected static final Logger logger = LoggerFactory.getLogger(NettyClient.class);

    private ServiceInformation serviceInformation;

    @JsonIgnore
    private InterlinkHandler interlinkHandler;

    private NettyDatagramUtils nettyDatagramUtils;

    @JsonIgnore
    private static ObjectMapper mapper = new ObjectMapper(){{
        this.enableDefaultTyping();
        this.setSerializationInclusion(JsonInclude.Include.NON_NULL);
    }};

    public NettyClient(ServiceInformation serviceInformation, InterlinkHandler interlinkHandler){
        this.serviceInformation = serviceInformation;
        this.interlinkHandler = interlinkHandler;
        this.nettyDatagramUtils = new NettyDatagramUtils(interlinkHandler);
    }

    public void add(String topic, NucleoData data){
        try {
            DatagramSocket socket = new DatagramSocket();
            String[] connectionInfo = serviceInformation.getConnectString().split(":");
            InetAddress address = InetAddress.getByName(connectionInfo[0]);
            try {
                nettyDatagramUtils.send(socket, new InterlinkMessage(topic, data), address, Integer.valueOf(connectionInfo[1]));
            }catch (Exception e){
                e.printStackTrace();
            }
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    @Override
    public boolean isConnected() {

        return false;
    }

    public ServiceInformation getServiceInformation() {
        return serviceInformation;
    }

    @Override
    public void close() {

    }

    @Override
    public void run() {
        logger.info("Netty interlink client created");
    }
}
