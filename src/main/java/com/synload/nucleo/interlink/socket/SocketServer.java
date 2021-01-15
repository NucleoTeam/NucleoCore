package com.synload.nucleo.interlink.socket;

import com.synload.nucleo.interlink.InterlinkHandler;
import com.synload.nucleo.interlink.InterlinkServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;

public class SocketServer implements InterlinkServer {
    protected static final Logger logger = LoggerFactory.getLogger(SocketServer.class);
    private ServerSocket server;
    private InterlinkHandler interlinkHandler;
    private Socket socket;

    public SocketServer(int port, InterlinkHandler interlinkHandler){
        this.interlinkHandler = interlinkHandler;
        try {
            server = new ServerSocket(port);
        }catch (IOException e){
            e.printStackTrace();
        }
    }

    public void run() {
        socket   = null;
        try {
            while ((socket = server.accept())!=null) {
                logger.info(socket.getInetAddress().getHostAddress() + " connected!");
                new Thread(new SocketReadHandler(socket, null, this)).start();
                Thread.sleep(10);
            }
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    public InterlinkHandler getInterlinkHandler() {
        return interlinkHandler;
    }

    @Override
    public void close() {
        try {
            socket.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
