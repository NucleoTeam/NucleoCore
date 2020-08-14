package com.synload.nucleo.socket;

import com.synload.nucleo.NucleoMesh;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;

public class EServer implements Runnable {
    protected static final Logger logger = LoggerFactory.getLogger(EServer.class);
    ServerSocket server;
    EManager em;
    NucleoMesh mesh;

    public EServer(int port, NucleoMesh mesh, EManager em){
        this.mesh = mesh;
        this.em = em;
        try {
            server = new ServerSocket(port);
        }catch (IOException e){
            e.printStackTrace();
        }
    }

    @Override
    public void run() {
        Socket socket   = null;
        try {
            while ((socket = server.accept())!=null) {
                logger.info(socket.getInetAddress().getHostAddress() + " connected!");
                new Thread(new EClient(socket, null, this.mesh)).start();
                Thread.sleep(10);
            }
        }catch (Exception e){
            e.printStackTrace();
        }
    }
}
