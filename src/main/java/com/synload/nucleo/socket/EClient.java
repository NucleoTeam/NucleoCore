package com.synload.nucleo.socket;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import com.google.common.collect.Queues;
import com.synload.nucleo.NucleoMesh;
import com.synload.nucleo.event.NucleoData;
import com.synload.nucleo.hub.Hub;
import com.synload.nucleo.zookeeper.ServiceInformation;
import org.apache.logging.log4j.core.util.IOUtils;

import java.io.*;
import java.math.BigInteger;
import java.net.ConnectException;
import java.net.Socket;
import java.net.SocketException;
import java.nio.ByteBuffer;
import java.util.Queue;
import java.util.Stack;
import java.util.concurrent.CountDownLatch;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

public class EClient implements Runnable {
    public ServiceInformation node;
    public boolean direction;
    public NucleoMesh mesh;
    public Socket client;
    public int streams = 0;
    public Queue<NucleoTopicPush> queue = Queues.newArrayDeque();
    private CountDownLatch countDownLatch = new CountDownLatch(1);
    public ObjectMapper mapper;
    public boolean reconnect = true;

    public static int readSize = 2048;

    public void add(String topic, NucleoData data){
        queue.add(new NucleoTopicPush(topic, data));
        countDownLatch.countDown();
    }
    public EClient(Socket client, ServiceInformation node, NucleoMesh mesh){
        this.node = node;
        this.mesh = mesh;
        this.client = client;
        this.direction = ( client != null );
        this.mapper = new ObjectMapper();
    }
    public synchronized NucleoTopicPush pop(){
        if(!queue.isEmpty()) {
            return queue.poll();
        }
        return null;
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
    public boolean readFromSock(int sizeRemaining, InputStream is, ByteArrayOutputStream output) throws IOException{
        try {
            byte[] buffer = new byte[readSize];
            output.reset();
            while (sizeRemaining > 0) {
                if (sizeRemaining < readSize) {
                    buffer = new byte[sizeRemaining];
                }
                sizeRemaining -= is.read(buffer, 0, sizeRemaining);
                output.write(buffer);

            }
            return true;
        }catch (Exception e){
            try {
                client.close();
                reconnect = false;
            }catch (Exception ex){
                ex.printStackTrace();
            }
        }
        return false;
    }
    @Override
    public void run() {
        try {
            while (reconnect && !Thread.currentThread().isInterrupted()) {
                if (this.direction) {
                    try {
                        BufferedInputStream is = new BufferedInputStream(client.getInputStream());
                        ByteArrayOutputStream output = new ByteArrayOutputStream();

                        byte[] buffer;
                        while (reconnect && !Thread.currentThread().isInterrupted() && !client.isClosed()) {
                            while (!client.isClosed()) {
                                // Get nucleodata
                                buffer = new byte[4];
                                is.read(buffer, 0, 4);
                                int sizeRemaining = ByteBuffer.wrap(buffer).getInt();

                                if(readFromSock(sizeRemaining, is, output)) {
                                    NucleoTopicPush data = mapper.readValue(output.toByteArray(), NucleoTopicPush.class);
                                    data.getData().markTime("Read from Socket");
                                    if (data.getData() != null) {
                                        mesh.getHub().handle(mesh.getHub(), data.getData(), data.getTopic());
                                    } else if (data.getInformation() != null) {
                                        System.out.println(data.getInformation().getName() + "." + data.getInformation().getService() + " " + data.getInformation().getHost());
                                    }
                                }
                            }
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
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
                    Socket clientLocal = null;
                    String[] connectArr = node.getConnectString().split(":");
                    NucleoTopicPush push = null;
                    try {
                        clientLocal = new Socket(connectArr[0], Integer.valueOf(connectArr[1]));
                        BufferedOutputStream gos = new BufferedOutputStream(clientLocal.getOutputStream());
                        while (!clientLocal.isClosed() && reconnect && !Thread.currentThread().isInterrupted()) {
                            countDownLatch.await();
                            countDownLatch = new CountDownLatch(1);
                            /*if (queue.size() > 15 && streams<10) {
                                streams();
                                new Thread(this).start();
                            }*/

                            while ((push = pop()) != null) {
                                //if (push.getTopic().startsWith("nucleo.client")) {
                                    //System.out.println("[ " + push.getTopic() + " ] " + push.getData().getRoot() + " -> " + node.getConnectString());
                                //}
                                push.getData().markTime("Write to Socket");
                                byte[] data = mapper.writeValueAsBytes(push);
                                gos.write(ByteBuffer.allocate(4).putInt(data.length).array());
                                gos.write(data);
                                gos.flush();
                            }
                        }
                    } catch (ConnectException c){
                        reconnect=false;
                        Thread.currentThread().interrupt();
                        c.printStackTrace();
                    } catch (SocketException e) {
                        if(push!=null){
                            this.getMesh().geteManager().robin(push.getTopic(), push.getData());
                        }
                        e.printStackTrace();
                    } catch (Exception e) {
                        e.printStackTrace();
                    } finally {
                        System.out.println(node.getConnectString() + " disconnected!");
                        if(clientLocal!=null){
                            clientLocal.close();
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
