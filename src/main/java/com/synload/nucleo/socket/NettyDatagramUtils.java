package com.synload.nucleo.socket;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.synload.nucleo.hub.Hub;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.ByteArrayOutputStream;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.util.*;

public class NettyDatagramUtils {
    private class Combiner{
        private int parts = -1;
        private byte[] array;
        public Combiner(int length) {
            this.array = new byte[length];
        }

        public int getParts() {
            return parts;
        }

        public void setParts(int parts) {
            this.parts = parts;
        }

        public byte[] getArray() {
            return array;
        }

        public void setArray(byte[] array) {
            this.array = array;
        }
        public void combine(NettyDatagramPart part){
            int i = 0;
            for(int x=part.getStart();x<part.getStart()+part.getBytes().length;x++){
                this.array[x] = part.getBytes()[i];
                i++;
            }
            parts++;
        }
    }
    public Map<String, Combiner> buffer = Maps.newHashMap();

    @JsonIgnore
    private static ObjectMapper mapper = new ObjectMapper(){{
        this.enableDefaultTyping();
        this.setSerializationInclusion(JsonInclude.Include.NON_NULL);
    }};

    protected static final Logger logger = LoggerFactory.getLogger(NettyDatagramUtils.class);

    public NettyDatagramUtils() {

    }

    public void send(DatagramSocket socket, byte[] data, InetAddress address, int port) {
        long left = data.length;
        long dataLen = left;
        int start = 0;
        int size = 500;
        int part = 0;
        int total = (int) Math.ceil(left / size);
        String uuid = UUID.randomUUID().toString();
        while (left > 0) {
            if (size > left) {
                size = (int)left;
            }
            byte[] byteDataPart = Arrays.copyOfRange(data, start, start + size);
            try {
                byte[] nettyDatagramPart = mapper.writeValueAsBytes(new NettyDatagramPart(byteDataPart, dataLen, start, uuid, part, total));
                socket.send(new DatagramPacket(nettyDatagramPart, nettyDatagramPart.length, address, port));
            } catch (Exception e) {
                e.printStackTrace();
            }
            start += size;
            left -= size;
            part++;
        }
    }

    public void allReceived(Hub hub, byte[] bytes) {
        try {
            NucleoTopicPush data = mapper.readValue(bytes, NucleoTopicPush.class);
            hub.handle(hub, data.getData(), data.getTopic());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void receivePacket(Hub hub, byte[] datagramPartBytes) {
        try {
            NettyDatagramPart nettyDatagramPart = mapper.readValue(datagramPartBytes, NettyDatagramPart.class);
            if (nettyDatagramPart.getTotal() == 1) {
                //logger.info(new String(nettyDatagramPart.getBytes()));
                allReceived(hub, nettyDatagramPart.getBytes());
            } else {
                String key = nettyDatagramPart.getRoot();
                if(!buffer.containsKey(key))
                    buffer.put(key, new Combiner((int)nettyDatagramPart.getLength()));
                Combiner combiner = buffer.get(key);
                combiner.combine(nettyDatagramPart);
                if(combiner.getParts()==nettyDatagramPart.getTotal()){
                    //logger.info(new String(combiner.getArray()));
                    allReceived(hub, combiner.getArray());
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
