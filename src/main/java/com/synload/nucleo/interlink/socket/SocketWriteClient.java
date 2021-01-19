package com.synload.nucleo.interlink.socket;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.synload.nucleo.data.NucleoData;
import com.synload.nucleo.interlink.InterlinkClient;
import com.synload.nucleo.interlink.InterlinkHandler;
import com.synload.nucleo.interlink.InterlinkMessage;
import com.synload.nucleo.zookeeper.ServiceInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.ConnectException;
import java.net.Socket;
import java.net.SocketException;
import java.nio.ByteBuffer;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class SocketWriteClient implements InterlinkClient  {

    @JsonIgnore
    protected static final Logger logger = LoggerFactory.getLogger(SocketWriteClient.class);

    public ServiceInformation serviceInformation;

    @JsonIgnore
    public Socket client;

    @JsonIgnore
    public int streams = 0;

    @JsonIgnore
    public BlockingQueue<InterlinkMessage> queue = new LinkedBlockingQueue<>();

    @JsonIgnore
    private ObjectMapper mapper;

    @JsonIgnore
    private InterlinkHandler interlinkHandler;


    public boolean reconnect = true;

    public void add(String topic, NucleoData data){
        queue.add(new InterlinkMessage(topic, data));
    }
    public SocketWriteClient(ServiceInformation serviceInformation, InterlinkHandler interlinkHandler){
        this.serviceInformation = serviceInformation;
        this.interlinkHandler = interlinkHandler;
        this.mapper = new ObjectMapper();
        this.mapper.enableDefaultTyping();
    }

    @Override
    public void run() {
        int tries = 0;
        try {
            while (reconnect && !Thread.currentThread().isInterrupted() && tries<30) {
                if(serviceInformation!=null) {
                    logger.info("Starting new connection to " + serviceInformation.getConnectString()+ " size:"+queue.size());
                }
                if(serviceInformation.getConnectString()==null) {
                    return;
                }
                String[] connectArr = serviceInformation.getConnectString().split(":");
                InterlinkMessage push = null;
                try {
                    client = new Socket(connectArr[0], Integer.valueOf(connectArr[1]));
                    BufferedOutputStream gos = new BufferedOutputStream(client.getOutputStream());
                    while (!client.isClosed() && reconnect && !Thread.currentThread().isInterrupted()) {
                        /*if (queue.size() > 15 && streams<10) {
                            streams();
                            new Thread(this).start();
                        }*/
                        while ((push = queue.poll(1000, TimeUnit.MILLISECONDS)) != null) {
                            /*if (push.getTopic().startsWith("nucleo.client")) {
                                System.out.println("[ " + push.getTopic() + " ] " + push.getData().getRoot() + " -> " + node.getConnectString());
                            }*/
                            //push.getData().markTime("Write to Socket");
                            byte[] data = new byte[0];
                            ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
                            ObjectOutputStream objectOutputStream = null;
                            try {
                                objectOutputStream = new ObjectOutputStream(byteArrayOutputStream);
                                objectOutputStream.writeObject(push);
                                objectOutputStream.flush();
                                data = byteArrayOutputStream.toByteArray();
                            } catch (IOException e) {
                                e.printStackTrace();
                            } finally {
                                try {
                                    if (byteArrayOutputStream != null)
                                        byteArrayOutputStream.close();
                                } catch (Exception e) {
                                    e.printStackTrace();
                                }
                                try {
                                    if (objectOutputStream != null)
                                        objectOutputStream.close();
                                } catch (Exception e) {
                                    e.printStackTrace();
                                }
                            }
                            if (data.length > 0){
                                gos.write(ByteBuffer.allocate(4).putInt(data.length).array());
                                gos.write(data);
                                gos.flush();
                            }
                        }
                    }
                } catch (ConnectException c){
                    reconnect=false;
                    Thread.currentThread().interrupt();
                    c.printStackTrace();
                } catch (SocketException e) {
                    logger.error("Failure sending through interlink ", e);
                    if(push!=null){
                        if(interlinkHandler!=null)
                            interlinkHandler.handleMessage(push.getTopic(), push.getData());
                    }
                    if(client!=null){
                        client.close();
                        client=null;
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                } finally {
                    logger.info(serviceInformation.getConnectString() + " disconnected!");
                    if(client!=null){
                        client.close();
                    }
                }
                tries++;
                try{
                    Thread.sleep(100,0);
                }catch (Exception e){
                    e.printStackTrace();
                }
            }
            logger.info("Retries expired");
        } catch (Exception e) {
            e.printStackTrace();
            serviceInformation=null;
        }
    }

    public ServiceInformation getServiceInformation() {
        return serviceInformation;
    }

    @Override
    public void close() {
        try {
            client.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void setServiceInformation(ServiceInformation serviceInformation) {
        this.serviceInformation = serviceInformation;
    }

    public Socket getClient() {
        return client;
    }
    public ObjectMapper getMapper() {
        return mapper;
    }

    public boolean isReconnect() {
        return reconnect;
    }

    public void setReconnect(boolean reconnect) {
        this.reconnect = reconnect;
    }

    public boolean isConnected(){
        if(client==null)
            return false;
        if(client.isClosed())
            return false;
        return client.isConnected();
    }



}
