package com.synload.nucleo.interlink.netty;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.socket.DatagramPacket;
import io.netty.util.ReferenceCountUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;


public class NettyIncomingHandler extends ChannelInboundHandlerAdapter {
    private NettyServer nettyServer;
    protected static final Logger logger = LoggerFactory.getLogger(NettyIncomingHandler.class);

    public NettyIncomingHandler(NettyServer nettyServer){
        this.nettyServer = nettyServer;
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        DatagramPacket in = (DatagramPacket) msg;
        try {
            final InetAddress srcAddr = in.sender().getAddress();
            final ByteBuf buf = in.content();
            final int rcvPktLength = buf.readableBytes();
            final byte[] rcvPktBuf = new byte[rcvPktLength];
            buf.readBytes(rcvPktBuf);
            getNettyServer().getNettyDatagramUtils().receivePacket(rcvPktBuf);
        } finally {
            ReferenceCountUtil.release(msg);
        }
    }
    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        cause.printStackTrace();
        ctx.close();
    }

    public NettyServer getNettyServer() {
        return nettyServer;
    }

    public void setNettyServer(NettyServer nettyServer) {
        this.nettyServer = nettyServer;
    }
}