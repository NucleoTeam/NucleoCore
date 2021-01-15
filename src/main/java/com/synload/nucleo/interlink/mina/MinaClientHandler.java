package com.synload.nucleo.interlink.mina;

import com.fasterxml.jackson.annotation.JsonIgnore;
import org.apache.mina.core.future.ConnectFuture;
import org.apache.mina.core.future.IoFuture;
import org.apache.mina.core.future.IoFutureListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MinaClientHandler implements IoFutureListener {

    @JsonIgnore
    protected static final Logger logger = LoggerFactory.getLogger(MinaClientHandler.class);

    MinaClient.SetSession setSession;

    public MinaClientHandler(MinaClient.SetSession setSession){
        this.setSession = setSession;
    }
    public void operationComplete(IoFuture future) {

        ConnectFuture connFuture = (ConnectFuture)future;
        if( connFuture.isConnected() ){
            setSession.set(future.getSession());
        } else {
            logger.error("Not connected...exiting");
        }
    }
}
