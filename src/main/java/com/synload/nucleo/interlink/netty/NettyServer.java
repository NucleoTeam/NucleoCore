package com.synload.nucleo.interlink.netty;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.synload.nucleo.interlink.InterlinkHandler;
import com.synload.nucleo.interlink.InterlinkServer;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioDatagramChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;

public class NettyServer implements InterlinkServer {
    @JsonIgnore
    protected static final Logger logger = LoggerFactory.getLogger(NettyServer.class);
    private int port;
    private NettyDatagramUtils nettyDatagramUtils;
    private ChannelFuture channelFuture;
    private InterlinkHandler interlinkHandler;

    public NettyServer(int port, InterlinkHandler interlinkHandler) {
        this.port = port;
        this.interlinkHandler = interlinkHandler;
        this.nettyDatagramUtils = new NettyDatagramUtils(interlinkHandler);
    }

    public void run() {
        final NioEventLoopGroup group = new NioEventLoopGroup();
        NettyServer nettyServer = this;
        try {
            final Bootstrap bootstrap = new Bootstrap();
            bootstrap.group(group).channel(NioDatagramChannel.class)
                .option(ChannelOption.SO_BROADCAST, true)
                .option(ChannelOption.SO_KEEPALIVE, true)
                .option(ChannelOption.SO_BACKLOG, 300)
                .handler(new ChannelInitializer<NioDatagramChannel>() {
                    @Override
                    public void initChannel(final NioDatagramChannel ch) throws Exception {
                        ChannelPipeline pipeline = ch.pipeline();
                        pipeline.addLast(new NettyIncomingHandler(nettyServer));
                    }
                });

            // Bind and start to accept incoming connections.
            Integer pPort = port;
            InetAddress address = InetAddress.getLocalHost();
            System.out.printf("waiting for message %s %s", String.format(pPort.toString()), String.format(address.toString()));
            channelFuture = bootstrap.bind(address, port).sync().channel().closeFuture();
            channelFuture.await();
        }catch (Exception e){
            e.printStackTrace();
        } finally {
            logger.info("Netty Server closed");
        }
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public NettyDatagramUtils getNettyDatagramUtils() {
        return nettyDatagramUtils;
    }

    public void setNettyDatagramUtils(NettyDatagramUtils nettyDatagramUtils) {
        this.nettyDatagramUtils = nettyDatagramUtils;
    }

    @Override
    public InterlinkHandler getInterlinkHandler() {
        return interlinkHandler;
    }

    @Override
    public void close() {
        channelFuture.cancel(true);
    }
}
