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
import java.util.concurrent.CountDownLatch;
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
    public byte[] compress(byte[] data) {
        byte[] compressed = new byte[0];
        GZIPOutputStream gzip = null;
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        try {
            gzip = new GZIPOutputStream(bos);
            gzip.write(data, 0, data.length);
            gzip.finish();
            compressed = bos.toByteArray();
        }catch (IOException e){
            e.printStackTrace();
        }finally {
            try {
                if (bos != null)
                    bos.close();
                if (gzip != null)
                    gzip.close();
            }catch (Exception ex){

            }
        }
        return compressed;
    }

    public byte[] decompress(byte[] compressed) {
        ByteArrayInputStream bais = new ByteArrayInputStream(compressed);
        GZIPInputStream gz = null;
        String content = "";
        try {
            gz = new GZIPInputStream(bais);
            InputStreamReader reader = new InputStreamReader(gz);
            BufferedReader in = new BufferedReader(reader);

            String read;
            while ((read = in.readLine()) != null) {
                content += read;
            }
        }catch (Exception ex){
            ex.printStackTrace();
        }finally {
            try {
                if (bais != null)
                    bais.close();
                if (gz != null)
                    gz.close();
            }catch (Exception ex){
                ex.printStackTrace();
            }
        }
        return content.getBytes();
    }
    public CountDownLatch latch = new CountDownLatch(1);
    @Override
    public void run() {
        try {
            while (reconnect) {
                if (this.direction) {
                    try {
                        if(client.isClosed()){
                            return;
                        }
                        DataInputStream is = new DataInputStream(client.getInputStream());
                        ByteArrayOutputStream output = new ByteArrayOutputStream();
                        byte[] buffer;
                        while (reconnect) {
                            if (is.available()>0) {

                                buffer = new byte[4];
                                is.read(buffer, 0, 4);
                                int sizeRemaining = ByteBuffer.wrap(buffer).getInt();

                                buffer = new byte[2048];
                                output.reset();
                                while(sizeRemaining>0){
                                    if(sizeRemaining<2048){
                                        buffer = new byte[sizeRemaining];
                                    }
                                    sizeRemaining -= is.read(buffer, 0, sizeRemaining);
                                    output.write(buffer);
                                }

                                NucleoData data = mapper.readValue(output.toByteArray(), NucleoData.class);
                                buffer = new byte[4];
                                is.read(buffer, 0, 4);
                                sizeRemaining = ByteBuffer.wrap(buffer).getInt();

                                buffer = new byte[2048];
                                output.reset();
                                while(sizeRemaining>0){
                                    if(sizeRemaining<2048){
                                        buffer = new byte[sizeRemaining];
                                    }
                                    sizeRemaining -= is.read(buffer, 0, sizeRemaining);
                                    output.write(buffer);
                                }
                                mesh.getHub().handle(mesh.getHub(), data, new String(output.toByteArray()));
                            }
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                    } finally {
                        client.close();
                    }
                } else {
                    String[] connectArr = node.getConnectString().split(":");
                    client = new Socket(connectArr[0], Integer.valueOf(connectArr[1]));
                    try {
                        DataOutputStream gos = new DataOutputStream(client.getOutputStream());
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
                            latch.await();
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                    } finally {
                        System.out.println("disconnected");
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

    public CountDownLatch getLatch() {
        return latch;
    }

    public void setLatch(CountDownLatch latch) {
        this.latch = latch;
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
