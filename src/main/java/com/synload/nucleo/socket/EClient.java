package com.synload.nucleo.socket;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.synload.nucleo.NucleoMesh;
import com.synload.nucleo.event.NucleoData;
import com.synload.nucleo.hub.Hub;
import com.synload.nucleo.zookeeper.ServiceInformation;
import org.apache.logging.log4j.core.util.IOUtils;

import java.io.*;
import java.net.ConnectException;
import java.net.Socket;
import java.net.SocketException;
import java.nio.ByteBuffer;
import java.util.Stack;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

public class EClient implements Runnable {
    public ServiceInformation node;
    public boolean direction;
    public NucleoMesh mesh;
    public Socket client;
    public int streams = 0;
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
    public synchronized NucleoTopicPush pop(){
        if(!queue.isEmpty()) {
            return queue.pop();
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
            byte[] buffer = new byte[2048];
            output.reset();
            while (sizeRemaining > 0) {
                if (sizeRemaining < 2048) {
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
        if(node!=null) {
            System.out.println("Starting new connection to " + node.getConnectString()+ " size:"+queue.size());
        }
        try {
            while (reconnect && !Thread.currentThread().isInterrupted()) {
                if (this.direction) {
                    try {
                        InputStream is = client.getInputStream();
                        ByteArrayOutputStream output = new ByteArrayOutputStream();

                        byte[] buffer;
                        while (reconnect && !Thread.currentThread().isInterrupted() && !client.isClosed()) {
                            while (is.available()>0 && !client.isClosed()) {
                                // Get nucleodata
                                buffer = new byte[4];
                                is.read(buffer, 0, 4);
                                int sizeRemaining = ByteBuffer.wrap(buffer).getInt();

                                if(readFromSock(sizeRemaining, is, output)) {
                                    NucleoTopicPush data = mapper.readValue(output.toByteArray(), NucleoTopicPush.class);
                                    if (data.getData() != null) {
                                        mesh.getHub().handle(mesh.getHub(), data.getData(), data.getTopic());
                                    } else if (data.getInformation() != null) {
                                        System.out.println(data.getInformation().getName() + "." + data.getInformation().getService() + " " + data.getInformation().getHost());
                                    }
                                }

                            }
                            Thread.sleep(0, 200);
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                    } finally {
                        client.close();
                        reconnect = false;
                        return;
                    }
                } else {
                    if(node.getConnectString()==null) {
                        return;
                    }
                    Socket clientLocal = null;
                    String[] connectArr = node.getConnectString().split(":");
                    NucleoTopicPush push = null;
                    try {
                        clientLocal = new Socket(connectArr[0], Integer.valueOf(connectArr[1]));
                        OutputStream gos = clientLocal.getOutputStream();
                        while (reconnect && !Thread.currentThread().isInterrupted()) {
                            if (clientLocal.isClosed()) {
                                Thread.currentThread().interrupt();
                                return;
                            }
                            if (queue.size() > 15 && streams<10) {
                                streams();
                                new Thread(this).start();
                            }
                            push = pop();
                            if (push != null) {
                                if (push.getTopic().startsWith("nucleo.client")) {
                                    System.out.println("[ " + push.getTopic() + " ] " + push.getData().getRoot() + " -> " + node.getConnectString());
                                }
                                byte[] data = mapper.writeValueAsBytes(push);
                                gos.write(ByteBuffer.allocate(4).putInt(data.length).array(), 0, 4);
                                gos.write(data, 0, data.length);
                                gos.flush();
                            }
                            Thread.sleep(0, 100);
                        }
                    } catch (ConnectException c){
                        reconnect=false;
                        Thread.currentThread().interrupt();
                    } catch (SocketException e) {
                        if(push!=null){
                            this.getMesh().geteManager().robin(push.getTopic(), push.getData());
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                    } finally {
                        System.out.println("disconnected");
                        if(clientLocal!=null){
                            clientLocal.close();
                        }
                        reconnect=false;
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


}
