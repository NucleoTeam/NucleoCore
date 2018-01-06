package com.synload.nucleo.server.socket.writer;

import com.synload.nucleo.server.socket.SocketClient;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.net.SocketException;
import java.util.List;
import org.apache.commons.io.IOUtils;

public class SocketWriter implements Runnable {
    private boolean keepRunning;
    private DataOutputStream output;
    private SocketClient client;
    public SocketWriter(DataOutputStream output, SocketClient client){
        this(output, client, true);
    }
    public SocketWriter(DataOutputStream output, SocketClient client, boolean keepRunning){
        this.output = output;
        this.keepRunning = keepRunning;
        this.client = client;
    }
    public void run() {
        try {
            while(keepRunning){
                if(this.getClient().getQueue().hasNext()){
                    Object data = this.getClient().getQueue().getRead();
                    if(data!=null) {
                        ByteArrayOutputStream arrayOut = new ByteArrayOutputStream();
                        ObjectOutputStream out;
                        try {
                            out = new ObjectOutputStream(arrayOut);
                            out.writeObject(data);
                            IOUtils.closeQuietly(out);
                            byte[] bytes = arrayOut.toByteArray();
                            IOUtils.closeQuietly(arrayOut);
                            output.writeInt(bytes.length);
                            output.write(bytes);
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                    }
                }
                Thread.sleep(10);
            }

        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public boolean isKeepRunning() {
        return keepRunning;
    }

    public void setKeepRunning(boolean keepRunning) {
        this.keepRunning = keepRunning;
    }

    public DataOutputStream getOutput() {
        return output;
    }

    public void setOutput(DataOutputStream output) {
        this.output = output;
    }

    public SocketClient getClient() {
        return client;
    }

    public void setClient(SocketClient client) {
        this.client = client;
    }
}
