package com.synload.nucleo.socket;

import com.synload.nucleo.NucleoMesh;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioDatagramChannel;

import java.net.InetAddress;

public class NettyServer implements Runnable {

    private int port;
    private EManager em;
    private NucleoMesh mesh;
    private NettyDatagramUtils nettyIncomingHandler;

    public NettyServer(int port, NucleoMesh mesh, EManager em) {
        this.port = port;
        this.mesh = mesh;
        this.em = em;
        this.nettyIncomingHandler = new NettyDatagramUtils();
    }

    public void run() {
        final NioEventLoopGroup group = new NioEventLoopGroup();
        NettyServer thisServer = this;
        try {
            final Bootstrap b = new Bootstrap();
            b.group(group).channel(NioDatagramChannel.class)
                .option(ChannelOption.SO_BROADCAST, true)
                .handler(new ChannelInitializer<NioDatagramChannel>() {
                    @Override
                    public void initChannel(final NioDatagramChannel ch) throws Exception {
                        ChannelPipeline p = ch.pipeline();
                        p.addLast(new NettyIncomingHandler(thisServer));
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

    public EManager getEm() {
        return em;
    }

    public void setEm(EManager em) {
        this.em = em;
    }

    public NucleoMesh getMesh() {
        return mesh;
    }

    public void setMesh(NucleoMesh mesh) {
        this.mesh = mesh;
    }

    public NettyDatagramUtils getNettyIncomingHandler() {
        return nettyIncomingHandler;
    }

    public void setNettyIncomingHandler(NettyDatagramUtils nettyIncomingHandler) {
        this.nettyIncomingHandler = nettyIncomingHandler;
    }
}
