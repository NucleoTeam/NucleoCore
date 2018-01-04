package com.synload.nucleo.server.socket;

import com.synload.nucleo.server.socket.reader.SocketReader;
import com.synload.nucleo.server.socket.writer.SocketWriter;
import com.synload.nucleo.utils.QueueList;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.util.LinkedList;
import java.util.Queue;
import java.util.Stack;
import java.util.UUID;

public class SocketClient implements Runnable {
    private Socket socket;
    private UUID identifier;
    private boolean connected;
    private QueueList queue = new QueueList();
    public SocketClient(Socket socket, UUID identifier){
        this.socket = socket;
        this.identifier=identifier;
    }
    public void run() {
        try{
            new Thread(
                new SocketReader(
                    new DataInputStream(
                        this.getSocket().getInputStream()
                    ),
                    this,
                    Thread.currentThread().getContextClassLoader()
                )
            ).start();
            new Thread(
                new SocketWriter(
                    new DataOutputStream(
                        this.getSocket().getOutputStream()
                    ),
                    this
                )
            ).start();
            while(this.getSocket().isConnected()){
                try {
                    Thread.sleep(1L);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            System.out.println("disconnected");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    public Socket getSocket() {
        return socket;
    }

    public void setSocket(Socket socket) {
        this.socket = socket;
    }

    public UUID getIdentifier() {
        return identifier;
    }

    public void setIdentifier(UUID identifier) {
        this.identifier = identifier;
    }

    public boolean isConnected() {
        return connected;
    }

    public void setConnected(boolean connected) {
        this.connected = connected;
    }

    public QueueList getQueue() {
        return queue;
    }
}
