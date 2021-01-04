package com.synload.nucleo.interlink.netty;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.synload.nucleo.NucleoMesh;
import com.synload.nucleo.interlink.InterlinkHandler;
import com.synload.nucleo.interlink.InterlinkManager;
import com.synload.nucleo.interlink.socket.SocketServerHandler;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioDatagramChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;

public class NettyServer implements Runnable {
    @JsonIgnore
    protected static final Logger logger = LoggerFactory.getLogger(NettyServer.class);

    private int port;
    private NettyDatagramUtils nettyIncomingHandler;
    private InterlinkHandler interlinkHandler;

    public NettyServer(int port, InterlinkHandler interlinkHandler) {
        this.port = port;
        this.nettyIncomingHandler = new NettyDatagramUtils();
        this.interlinkHandler = interlinkHandler;
    }

    public void run() {
        final NioEventLoopGroup group = new NioEventLoopGroup();
        NettyServer thisServer = this;
        try {
            final Bootstrap b = new Bootstrap();
            b.group(group).channel(NioDatagramChannel.class)
                .option(ChannelOption.SO_BROADCAST, true)
                .option(ChannelOption.SO_KEEPALIVE, true)
                .option(ChannelOption.SO_BACKLOG, 300)
                .handler(new ChannelInitializer<NioDatagramChannel>() {
                    @Override
                    public void initChannel(final NioDatagramChannel ch) throws Exception {
                        ChannelPipeline pipeline = ch.pipeline();
                        pipeline.addLast(new NettyIncomingHandler(thisServer));
                    }
                });

            // Bind and start to accept incoming connections.
            Integer pPort = port;
            InetAddress address = InetAddress.getLocalHost();
            System.out.printf("waiting for message %s %s", String.format(pPort.toString()), String.format(address.toString()));
            b.bind(address, port).sync().channel().closeFuture().await();
        }catch (Exception e){
            e.printStackTrace();
        } finally {
            System.out.print("In Server Finally");
        }
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public NettyDatagramUtils getNettyIncomingHandler() {
        return nettyIncomingHandler;
    }

    public void setNettyIncomingHandler(NettyDatagramUtils nettyIncomingHandler) {
        this.nettyIncomingHandler = nettyIncomingHandler;
    }

    public InterlinkHandler getInterlinkHandler() {
        return interlinkHandler;
    }
}
