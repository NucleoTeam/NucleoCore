package com.synload.nucleo.socket;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.synload.nucleo.NucleoMesh;
import com.synload.nucleo.event.NucleoData;
import com.synload.nucleo.hub.Hub;
import com.synload.nucleo.zookeeper.ServiceInformation;
import org.apache.logging.log4j.core.util.IOUtils;

import java.io.*;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.Stack;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

public class EClient implements Runnable {
    public ServiceInformation node;
    public boolean direction;
    public NucleoMesh mesh;
    public Socket client;
    public Stack<NucleoTopicPush> queue = new Stack<>();
    public ObjectMapper mapper;
    public boolean reconnect = true;

    public void add(String topic, NucleoData data){
        queue.add(new NucleoTopicPush(topic, data));
    }
    public EClient(Socket client, ServiceInformation node, NucleoMesh mesh){
        this.node = node;
        this.mesh = mesh;
        this.client = client;
        this.direction = ( client != null );
        this.mapper = new ObjectMapper();
    }

    @Override
    public void run() {
        try {
            while (reconnect) {
                if (this.direction) {
                    try {
                        GZIPInputStream is = new GZIPInputStream(new DataInputStream(client.getInputStream()));
                        String objRead = "";
                        byte[] buffer = new byte[4];
                        while (reconnect) {
                            if (is.available() > 0) {
                                is.read(buffer);
                                int sizeRemaining = ByteBuffer.wrap(buffer).getInt();
                                buffer = new byte[2048];
                                ByteBuffer buff = ByteBuffer.allocate(sizeRemaining);
                                while(sizeRemaining>0){
                                    if(sizeRemaining<2048){
                                        buffer = new byte[sizeRemaining];
                                    }
                                    is.read(buffer);
                                    buff.put(buffer);
                                    sizeRemaining -= 2048;
                                }
                                is.read(buffer);
                                NucleoData data = mapper.readValue(buff.array(), NucleoData.class);

                                buffer = new byte[4];
                                is.read(buffer);
                                sizeRemaining = ByteBuffer.wrap(buffer).getInt();
                                buffer = new byte[2048];
                                buff = ByteBuffer.allocate(sizeRemaining);
                                while(sizeRemaining>0){
                                    if(sizeRemaining<2048){
                                        buffer = new byte[sizeRemaining];
                                    }
                                    is.read(buffer);
                                    buff.put(buffer);
                                    sizeRemaining -= 2048;
                                }
                                mesh.getHub().handle(mesh.getHub(), data, new String(buff.array()));
                                buffer = new byte[2048];
                            }
                            Thread.sleep(1L);
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                    } finally {
                        client.close();
                    }
                } else {
                    String[] connectArr = node.getConnectString().split(":");
                    System.out.println("Connecting to " + connectArr[0] + " on port " + connectArr[1]);
                    client = new Socket(connectArr[0], Integer.valueOf(connectArr[1]));
                    try {
                        DataOutputStream os = new DataOutputStream(client.getOutputStream());
                        GZIPOutputStream gos = new GZIPOutputStream(os);
                        while (reconnect) {
                            if (!queue.empty()) {
                                NucleoTopicPush push = queue.pop();
                                byte[] data = mapper.writeValueAsBytes(push.getData());
                                gos.write(ByteBuffer.allocate(4).putInt(data.length).array());
                                gos.write(data);
                                byte[] topic = push.getTopic().getBytes();
                                gos.write(ByteBuffer.allocate(4).putInt(topic.length).array());
                                gos.write(topic);
                            }
                            Thread.sleep(1L);
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                    } finally {
                        client.close();
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public ServiceInformation getNode() {
        return node;
    }

    public void setNode(ServiceInformation node) {
        this.node = node;
    }

    public boolean isDirection() {
        return direction;
    }

    public void setDirection(boolean direction) {
        this.direction = direction;
    }

    public NucleoMesh getMesh() {
        return mesh;
    }

    public void setMesh(NucleoMesh mesh) {
        this.mesh = mesh;
    }

    public Socket getClient() {
        return client;
    }

    public void setClient(Socket client) {
        this.client = client;
    }

    public Stack<NucleoTopicPush> getQueue() {
        return queue;
    }

    public void setQueue(Stack<NucleoTopicPush> queue) {
        this.queue = queue;
    }

    public ObjectMapper getMapper() {
        return mapper;
    }

    public void setMapper(ObjectMapper mapper) {
        this.mapper = mapper;
    }

    public boolean isReconnect() {
        return reconnect;
    }

    public void setReconnect(boolean reconnect) {
        this.reconnect = reconnect;
    }

    public class NucleoTopicPush{
        NucleoData data = null;
        String topic = null;
        public NucleoTopicPush(String topic, NucleoData data){
            this.topic = topic;
            this.data = data;
        }
        public NucleoData getData() {
            return data;
        }

        public void setData(NucleoData data) {
            this.data = data;
        }

        public String getTopic() {
            return topic;
        }

        public void setTopic(String topic) {
            this.topic = topic;
        }
    }
}
