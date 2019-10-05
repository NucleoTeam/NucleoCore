package com.synload.nucleo.socket;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Queues;
import com.synload.nucleo.NucleoMesh;
import com.synload.nucleo.event.NucleoData;
import com.synload.nucleo.zookeeper.ServiceInformation;
import java.io.*;
import java.net.ConnectException;
import java.net.Socket;
import java.net.SocketException;
import java.nio.ByteBuffer;
import java.util.Queue;
import java.util.concurrent.CountDownLatch;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

public class EClient implements Runnable {

    public ServiceInformation node;

    @JsonIgnore
    public boolean direction;

    @JsonIgnore
    public NucleoMesh mesh;

    @JsonIgnore
    public Socket client;

    @JsonIgnore
    public int streams = 0;

    @JsonIgnore
    public Queue<NucleoTopicPush> queue = Queues.newArrayDeque();

    @JsonIgnore
    private CountDownLatch countDownLatch = new CountDownLatch(1);

    @JsonIgnore
    public ObjectMapper mapper;


    public boolean reconnect = true;
    public static int readSize = 1024;

    public void add(String topic, NucleoData data){
        synchronized (queue) {
            queue.add(new NucleoTopicPush(topic, data));
            countDownLatch.countDown();
        }
    }
    public EClient(Socket client, ServiceInformation node, NucleoMesh mesh){
        this.node = node;
        this.mesh = mesh;
        this.client = client;
        this.direction = ( client != null );
        this.mapper = new ObjectMapper();
    }
    private synchronized void streams(){
        streams++;
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
                ex.printStackTrace();
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
    public ByteArrayOutputStream readFromSock(int sizeRemaining, InputStream is) throws IOException{
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        try {
            byte[] buffer;
            output.reset();
            while (sizeRemaining > 0) {
                if (sizeRemaining < readSize) {
                    buffer = new byte[sizeRemaining];
                    sizeRemaining -= is.read(buffer, 0, sizeRemaining);
                }else{
                    buffer = new byte[readSize];
                    sizeRemaining -= is.read(buffer, 0, readSize);
                }
                output.write(buffer);
            }
            return output;
        }catch (Exception e){
            e.printStackTrace();
            try {
                client.close();
                reconnect = false;
            }catch (Exception ex){
                ex.printStackTrace();
            }
        }
        return output;
    }
    @Override
    public void run() {
        try {
            while (reconnect && !Thread.currentThread().isInterrupted()) {
                if (this.direction) {
                    try {
                        BufferedInputStream is = new BufferedInputStream(client.getInputStream());
                        byte[] buffer;
                        while (reconnect && !Thread.currentThread().isInterrupted() && !client.isClosed()) {
                            while (!client.isClosed()) {
                                // Get nucleodata
                                buffer = new byte[4];
                                is.read(buffer, 0, 4);
                                int sizeRemaining = ByteBuffer.wrap(buffer).getInt();
                                ByteArrayOutputStream output = readFromSock(sizeRemaining, is);
                                if(output.size()==sizeRemaining) {
                                    NucleoTopicPush data = mapper.readValue(output.toByteArray(), NucleoTopicPush.class);
                                    //System.out.println("read: "+data.getData().getRoot().toString());
                                    data.getData().markTime("Read from Socket");
                                    if (data.getData() != null) {
                                        mesh.getHub().handle(mesh.getHub(), data.getData(), data.getTopic());
                                    } else if (data.getInformation() != null) {
                                        System.out.println(data.getInformation().getName() + "." + data.getInformation().getService() + " " + data.getInformation().getHost());
                                    }
                                }else{
                                    System.exit(-1);
                                }
                            }
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                        //System.out.println(new String(output.toByteArray()));
                    } finally {
                        client.close();
                        reconnect = false;
                        return;
                    }
                } else { // Connecting to service
                    if(node!=null) {
                        System.out.println("Starting new connection to " + node.getConnectString()+ " size:"+queue.size());
                    }
                    if(node.getConnectString()==null) {
                        return;
                    }
                    String[] connectArr = node.getConnectString().split(":");
                    NucleoTopicPush push = null;
                    try {
                        client = new Socket(connectArr[0], Integer.valueOf(connectArr[1]));
                        BufferedOutputStream gos = new BufferedOutputStream(client.getOutputStream());
                        while (!client.isClosed() && reconnect && !Thread.currentThread().isInterrupted()) {
                            /*if (queue.size() > 15 && streams<10) {
                                streams();
                                new Thread(this).start();
                            }*/
                            countDownLatch.await();
                            synchronized (queue) {
                                while (!queue.isEmpty()) {
                                    push = queue.remove();
                                    /*if (push.getTopic().startsWith("nucleo.client")) {
                                        System.out.println("[ " + push.getTopic() + " ] " + push.getData().getRoot() + " -> " + node.getConnectString());
                                    }*/
                                    synchronized (push) {
                                        push.getData().markTime("Write to Socket");
                                        byte[] data = mapper.writeValueAsBytes(push);
                                        gos.write(ByteBuffer.allocate(4).putInt(data.length).array());
                                        gos.write(data);
                                        gos.flush();
                                    }
                                }
                            }

                        }
                    } catch (ConnectException c){
                        reconnect=false;
                        Thread.currentThread().interrupt();
                        c.printStackTrace();
                    } catch (SocketException e) {
                        if(push!=null){
                            mesh.geteManager().robin(push.getTopic(), push.getData());
                        }
                        if(client!=null){
                            client.close();
                            client=null;
                        }
                        e.printStackTrace();
                    } catch (Exception e) {
                        e.printStackTrace();
                    } finally {
                        System.out.println(node.getConnectString() + " disconnected!");
                        if(client!=null){
                            client.close();
                        }
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            node=null;
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

    public Queue<NucleoTopicPush> getQueue() {
        return queue;
    }

    public void setQueue(Queue<NucleoTopicPush> queue) {
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


}
