package com.synload.nucleo.socket;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.socket.DatagramPacket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;

public class NettyIncomingHandler extends SimpleChannelInboundHandler<DatagramPacket> {
    private NettyServer nettyServer;
    protected static final Logger logger = LoggerFactory.getLogger(NettyIncomingHandler.class);

    public NettyIncomingHandler(NettyServer nettyServer){
        this.nettyServer = nettyServer;
    }

    @Override
    protected void messageReceived(ChannelHandlerContext channelHandlerContext, DatagramPacket packet) throws Exception {
        final ByteBuf buf = packet.content();
        final int rcvPktLength = buf.readableBytes();
        final byte[] rcvPktBuf = new byte[rcvPktLength];
        buf.readBytes(rcvPktBuf);
        getNettyServer().getNettyIncomingHandler().receivePacket(this.getNettyServer().getMesh().getHub(), rcvPktBuf);

    }

    public NettyServer getNettyServer() {
        return nettyServer;
    }

    public void setNettyServer(NettyServer nettyServer) {
        this.nettyServer = nettyServer;
    }
}