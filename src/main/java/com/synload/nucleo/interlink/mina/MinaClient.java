package com.synload.nucleo.interlink.mina;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.synload.nucleo.data.NucleoData;
import com.synload.nucleo.interlink.InterlinkClient;
import com.synload.nucleo.interlink.InterlinkHandler;
import com.synload.nucleo.interlink.InterlinkMessage;
import com.synload.nucleo.zookeeper.ServiceInformation;
import org.apache.mina.core.future.ConnectFuture;
import org.apache.mina.core.service.IoConnector;
import org.apache.mina.core.service.IoHandler;
import org.apache.mina.core.session.IdleStatus;
import org.apache.mina.core.session.IoSession;
import org.apache.mina.filter.FilterEvent;
import org.apache.mina.filter.codec.ProtocolCodecFilter;
import org.apache.mina.filter.codec.serialization.ObjectSerializationCodecFactory;
import org.apache.mina.transport.socket.nio.NioSocketConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class MinaClient implements InterlinkClient {

    @JsonIgnore
    public BlockingQueue<InterlinkMessage> queue = new LinkedBlockingQueue<>();

    @JsonIgnore
    protected static final Logger logger = LoggerFactory.getLogger(MinaClient.class);

    private ServiceInformation serviceInformation;

    @JsonIgnore
    private InterlinkHandler interlinkHandler;

    private IoConnector connector;

    private IoSession session;

    public static interface SetSession {
        void set(IoSession session);
    }

    ConnectFuture connFuture;

    public MinaClient(ServiceInformation serviceInformation, InterlinkHandler interlinkHandler){
        this.serviceInformation = serviceInformation;
        this.interlinkHandler = interlinkHandler;

        connector = new NioSocketConnector();
        String[] connectionInfo = serviceInformation.getConnectString().split(":");
        connector.setHandler(new IoHandler() {
            @Override
            public void sessionCreated(IoSession session) throws Exception {
                logger.debug("Session created");
            }

            @Override
            public void sessionOpened(IoSession session) throws Exception {
                logger.debug("Session opened");
            }

            @Override
            public void sessionClosed(IoSession session) throws Exception {
                logger.debug("Session closed");
            }

            @Override
            public void sessionIdle(IoSession session, IdleStatus status) throws Exception {
                logger.debug("Session idle");
            }

            @Override
            public void exceptionCaught(IoSession session, Throwable cause) throws Exception {
                cause.printStackTrace();
            }

            @Override
            public void messageReceived(IoSession session, Object message) throws Exception {
                logger.debug("message received");
            }

            @Override
            public void messageSent(IoSession session, Object message) throws Exception {
                logger.debug("message sent");
            }

            @Override
            public void inputClosed(IoSession session) throws Exception {

            }

            @Override
            public void event(IoSession session, FilterEvent event) throws Exception {

            }
        });
        connector.getFilterChain().addLast( "codec", new ProtocolCodecFilter( new ObjectSerializationCodecFactory()));
        connector.getSessionConfig().setMinReadBufferSize( 1024 );
        connector.getSessionConfig().setMaxReadBufferSize( 10240 );
        connFuture = connector.connect( new InetSocketAddress(connectionInfo[0], Integer.valueOf(connectionInfo[1])));

        connFuture.addListener(new MinaClientHandler(session->this.session=session));
    }

    @Override
    public void add(String topic, NucleoData data) {

        queue.add(new InterlinkMessage(topic, data));
    }

    @Override
    public boolean isConnected() {
        return connFuture.isConnected();
    }

    @Override
    public ServiceInformation getServiceInformation() {
        return serviceInformation;
    }

    @Override
    public void close() {
        connFuture.cancel();
        session.closeOnFlush();
    }

    @Override
    public void run() {
        try {
            InterlinkMessage push;
            while(!Thread.currentThread().isInterrupted()) {
                while ((push = queue.poll(1000, TimeUnit.MILLISECONDS)) != null) {
                    if (session != null) {
                        session.write(push);
                    } else {
                        interlinkHandler.handleMessage(push.getTopic(), push.getData());
                    }
                }
            }
        }catch (Exception e){
            e.printStackTrace();
        }
    }
}
