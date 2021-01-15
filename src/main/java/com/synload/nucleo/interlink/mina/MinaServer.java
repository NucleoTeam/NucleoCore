package com.synload.nucleo.interlink.mina;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.synload.nucleo.interlink.InterlinkHandler;
import com.synload.nucleo.interlink.InterlinkServer;
import org.apache.mina.core.service.IoAcceptor;
import org.apache.mina.core.session.IdleStatus;
import org.apache.mina.filter.codec.ProtocolCodecFilter;
import org.apache.mina.filter.codec.serialization.ObjectSerializationCodecFactory;
import org.apache.mina.transport.socket.nio.NioSocketAcceptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;

public class MinaServer implements InterlinkServer {
    @JsonIgnore
    protected static final Logger logger = LoggerFactory.getLogger(MinaServer.class);

    private int port;
    private InterlinkHandler interlinkHandler;
    private IoAcceptor acceptor;

    public MinaServer(int port, InterlinkHandler interlinkHandler){
        this.port = port;
        this.interlinkHandler = interlinkHandler;
    }
    @Override
    public InterlinkHandler getInterlinkHandler() {
        return null;
    }

    @Override
    public void close() {
        acceptor.unbind();
    }

    @Override
    public void run() {

        acceptor = new NioSocketAcceptor(); // TCP Server

        //acceptor.getFilterChain().addLast( "logger", new LoggingFilter() );
        acceptor.getFilterChain().addLast( "codec", new ProtocolCodecFilter( new ObjectSerializationCodecFactory()));

        acceptor.setHandler( new MinaServerHandler(interlinkHandler) );
        //acceptor.getSessionConfig().setReadBufferSize( 1024*5 );
        acceptor.getSessionConfig().setIdleTime( IdleStatus.BOTH_IDLE, 10 );
        try {
            acceptor.bind( new InetSocketAddress(port) );
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
