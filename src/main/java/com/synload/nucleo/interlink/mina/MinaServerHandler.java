package com.synload.nucleo.interlink.mina;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.synload.nucleo.interlink.InterlinkHandler;
import com.synload.nucleo.interlink.InterlinkMessage;
import org.apache.mina.core.service.IoHandlerAdapter;
import org.apache.mina.core.session.IdleStatus;
import org.apache.mina.core.session.IoSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MinaServerHandler extends IoHandlerAdapter {
    @JsonIgnore
    protected static final Logger logger = LoggerFactory.getLogger(MinaServer.class);

    private InterlinkHandler interlinkHandler;
    public MinaServerHandler(InterlinkHandler interlinkHandler){
        this.interlinkHandler = interlinkHandler;
    }

    @Override
    public void exceptionCaught(IoSession session, Throwable cause ) throws Exception
    {
        cause.printStackTrace();
    }

    @Override
    public void messageReceived( IoSession session, Object message ) throws Exception
    {
        InterlinkMessage interlinkMessage =  (InterlinkMessage) message;
        interlinkHandler.handleMessage(interlinkMessage.getTopic(), interlinkMessage.getData());
    }

    @Override
    public void sessionIdle( IoSession session, IdleStatus status ) throws Exception
    {
        logger.info( "IDLE " + session.getIdleCount( status ));
    }
}
