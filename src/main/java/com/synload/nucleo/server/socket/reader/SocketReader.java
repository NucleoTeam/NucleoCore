package com.synload.nucleo.server.socket.reader;

import com.synload.nucleo.loader.EventClassLoader;
import com.synload.nucleo.server.socket.SocketClient;
import com.synload.nucleo.test.Data;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.LinkedList;
import java.util.Queue;
import java.util.Stack;

public class SocketReader implements Runnable {
    private Queue<Object> queue = new LinkedList<>();
    private boolean keepRunning;
    private ClassLoader cl;
    private SocketClient client;
    private DataInputStream input;
    public SocketReader(DataInputStream input, SocketClient client, ClassLoader cl){
        this(input, client, true, cl);
    }
    public SocketReader(DataInputStream input, SocketClient client, boolean keepRunning, ClassLoader cl){
        this.input = input;
        this.keepRunning = keepRunning;
        this.cl = cl;
        this.client = client;
    }
    public Object read(int length) throws IOException{
        int reading = 1024*1024*8;
        ByteArrayOutputStream bdata = new ByteArrayOutputStream();
        while(length>0){
            reading = 1024*1024*8;
            if(reading>length){
                reading=length;
            }
            byte[] message = new byte[reading];
            input.readFully(message);
            length -= reading;
            bdata.write(message);
        }
        if(bdata.size()>0){
            ByteArrayInputStream bas = new ByteArrayInputStream(bdata.toByteArray());
            ObjectLoader in = new ObjectLoader(cl, bas);
            try {
                Object obj = in.readObject();
                return obj;
            } catch (ClassNotFoundException e) {
                e.printStackTrace();
            }
        }
        return null;
    }
    public void run() {
        try {
            while (keepRunning) {
                if(input.available()>0) {
                    int length = input.readInt();

                    if (length > 0) {
                        Object data = read(length);
                        //System.out.println("length: "+length);
                        if(data!=null){
                            System.out.println("Communication received");

                            if(Data.class.isInstance(data)){
                                Data t = (Data) data;
                                //System.out.println(t.string);
                                t.string="changed string";
                                this.getClient().getQueue().add(t);
                            }

                        }else{
                            System.out.println("Unknown communication received");
                        }
                    }
                }
                Thread.sleep(1L);
            }
        }catch (InterruptedException e){
            e.printStackTrace();
        }catch (IOException e){
            e.printStackTrace();
        }
    }

    public Queue<Object> getQueue() {
        return queue;
    }

    public void setQueue(LinkedList<Object> queue) {
        this.queue = queue;
    }

    public boolean isKeepRunning() {
        return keepRunning;
    }

    public void setKeepRunning(boolean keepRunning) {
        this.keepRunning = keepRunning;
    }

    public DataInputStream getInput() {
        return input;
    }

    public void setInput(DataInputStream input) {
        this.input = input;
    }

    public ClassLoader getCl() {
        return cl;
    }

    public void setCl(ClassLoader cl) {
        this.cl = cl;
    }

    public SocketClient getClient() {
        return client;
    }

    public void setClient(SocketClient client) {
        this.client = client;
    }
}
