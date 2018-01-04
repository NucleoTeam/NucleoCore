package com.synload.nucleo.server;
import com.synload.nucleo.server.socket.SocketClient;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.UUID;

public class NucleoServer implements Runnable  {
    private int port;
    private UUID identifier;
    public NucleoServer(int port){
        this.port = port;
        identifier = UUID.randomUUID();
    }
    public void run() {
        try {
            ServerSocket server = new ServerSocket(port);
            Socket socket;
            while( (socket = server.accept()) != null ){
                System.out.println("socket connected");
                (new Thread(new SocketClient( socket, identifier ))).start();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
