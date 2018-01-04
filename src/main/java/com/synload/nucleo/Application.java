package com.synload.nucleo;

import com.synload.nucleo.server.NucleoServer;
import com.synload.nucleo.server.socket.SocketClient;
import com.synload.nucleo.test.Data;

import java.net.Socket;
import java.util.UUID;

public class Application {
    public static void main(String[] args){
        System.out.println("Starting server");
        NucleoServer server = new NucleoServer(8080);
        new Thread(server).start();
        try {
            Thread.sleep(1500);
            Socket clientSocket = new Socket("127.0.0.1", 8080);
            SocketClient sc = new SocketClient(clientSocket, UUID.randomUUID());
            new Thread(sc).start();
            sc.getQueue().add(new Data());
            sc.getQueue().add(new Data());
            sc.getQueue().add(new Data());
        }catch (Exception e){
            e.printStackTrace();
        }
    }
}
