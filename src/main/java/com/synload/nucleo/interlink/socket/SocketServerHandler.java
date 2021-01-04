package com.synload.nucleo.interlink.socket;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.synload.nucleo.interlink.InterlinkServer;
import com.synload.nucleo.interlink.InterlinkMessage;
import com.synload.nucleo.zookeeper.ServiceInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.Socket;
import java.nio.ByteBuffer;

public class SocketServerHandler implements Runnable{

    @JsonIgnore
    protected static final Logger logger = LoggerFactory.getLogger(SocketServerHandler.class);

    @JsonIgnore
    public ObjectMapper mapper;

    public ServiceInformation node;

    @JsonIgnore
    public InterlinkServer interlinkServer;

    @JsonIgnore
    public Socket client;

    public static int readSize = 1024;



    SocketServerHandler(Socket client, ServiceInformation node, InterlinkServer interlinkServer) {
        this.node = node;
        this.interlinkServer = interlinkServer;
        this.client = client;
        this.mapper = new ObjectMapper();
        this.mapper.enableDefaultTyping();
    }

    @Override
    public void run() {
        try {
            while (!Thread.currentThread().isInterrupted()) {
                ByteArrayOutputStream output = null;
                try {
                    BufferedInputStream is = new BufferedInputStream(client.getInputStream());
                    byte[] buffer;
                    while (!client.isClosed() && !Thread.currentThread().isInterrupted()) {
                        // Get nucleodata
                        buffer = new byte[4];
                        is.read(buffer, 0, 4);
                        int sizeRemaining = ByteBuffer.wrap(buffer).getInt();
                        output = readFromSock(sizeRemaining, is);
                        if (output.size() == sizeRemaining) {
                            InterlinkMessage data = mapper.readValue(output.toByteArray(), InterlinkMessage.class);
                            //System.out.println("read: "+data.getData().getRoot().toString());
                            //data.getData().markTime("Read from Socket");
                            if (data.getData() != null) {
                                interlinkServer.getInterlinkHandler().handleMessage(data.getTopic(), data.getData());
                            } else if (data.getInformation() != null) {
                                logger.info(data.getInformation().getName() + "." + data.getInformation().getService() + " " + data.getInformation().getHost());
                            }
                        } else {
                            System.exit(-1);
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                    if (output != null) {
                        logger.info(new String(output.toByteArray()));
                    }
                } finally {
                    client.close();
                    return;
                }
            }
            logger.info("Retries expired");
        } catch (Exception e) {
            e.printStackTrace();
            node = null;
        }
    }

    public ByteArrayOutputStream readFromSock(int sizeRemaining, InputStream is) throws IOException {
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
            }catch (Exception ex){
                ex.printStackTrace();
            }
        }
        return output;
    }
}
