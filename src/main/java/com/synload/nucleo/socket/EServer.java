package com.synload.nucleo.socket;

import com.synload.nucleo.NucleoMesh;
import com.synload.nucleo.event.NucleoData;
import com.synload.nucleo.zookeeper.ServiceInformation;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Stack;

public class EServer implements Runnable {
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
                System.out.println("connected!");
                new Thread(new EClient(socket, null, this.mesh)).start();
            }
        }catch (Exception e){
            e.printStackTrace();
        }
    }
}
