package com.synload.nucleo.socket;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.synload.nucleo.NucleoMesh;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.util.CharsetUtil;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.time.Clock;
import java.time.Instant;

public class ChannelServer implements Runnable {
    EManager em;
    NucleoMesh mesh;
    int port;

    public ChannelServer(int port, NucleoMesh mesh, EManager em){
        this.mesh = mesh;
        this.em = em;
        this.port = port;
    }
    public static void read(NucleoMesh mesh, byte[] inData){
        try {
            NucleoTopicPush push = new ObjectMapper().readValue(inData, NucleoTopicPush.class);
            //push.getData().markTime("Read from Socket");
            if (push.getData() != null) {
                mesh.getHub().handle(mesh.getHub(), push.getData(), push.getTopic());
            } else if (push.getInformation() != null) {
                System.out.println(push.getInformation().getName() + "." + push.getInformation().getService() + " " + push.getInformation().getHost());
            }
        }catch (Exception e){
            System.out.println(new String(inData));
            e.printStackTrace();
        }
    }
    public class ServerReadHandler extends ChannelInboundHandlerAdapter {
        int readLeft = -1;
        int totalSize = 0;
        byte[] buffer = null;
        void data(ByteBuf inBuffer){
            int available = inBuffer.readableBytes();
            //System.out.println("Available to read: "+available);
            //System.out.println("Reading state: [left: "+readLeft+"],[totalSize: "+totalSize+"],[available: "+available+"]");
            while(available>0) {
                if (readLeft == -1) {
                    //System.out.println("Reading new request");
                    if (available >= 4) {
                        byte[] intArray = new byte[4];
                        inBuffer.readBytes(intArray, 0, 4);
                        readLeft = ByteBuffer.wrap(intArray).getInt();
                        //System.out.println("Reading object: "+readLeft);
                        available=inBuffer.readableBytes();
                        //System.out.println("Available to read after int: "+available);
                        totalSize = readLeft;
                        buffer = new byte[readLeft];
                        if (readLeft > available) {
                            inBuffer.readBytes(buffer, (totalSize - readLeft), available);
                            readLeft = readLeft - available;
                            available = 0;
                            //System.out.println("Reading state: [left: "+readLeft+"],[totalSize: "+totalSize+"],[available: "+available+"]");
                        }else{
                            inBuffer.readBytes(buffer, (totalSize - readLeft), readLeft);
                            available=inBuffer.readableBytes();
                            readLeft = 0;
                            //System.out.println("Reading state: [left: "+readLeft+"],[totalSize: "+totalSize+"],[available: "+available+"]");
                        }
                    }
                } else if (readLeft > 0) {
                    //System.out.println("Resuming reading object: "+readLeft);
                    if (readLeft > available) {
                        inBuffer.readBytes(buffer, (totalSize - readLeft), available);
                        readLeft = readLeft- available;
                        available = 0;
                        //System.out.println("Reading state: [left: "+readLeft+"],[totalSize: "+totalSize+"],[available: "+available+"]");
                    }else{
                        inBuffer.readBytes(buffer, (totalSize - readLeft), readLeft);
                        available=inBuffer.readableBytes();
                        readLeft = 0;
                        //System.out.println("Reading state: [left: "+readLeft+"],[totalSize: "+totalSize+"],[available: "+available+"]");
                    }
                }
                if(readLeft==0){
                    read(mesh, buffer);
                    readLeft=-1;
                    totalSize=0;
                }
            }
        }
        @Override
        public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
            data((ByteBuf) msg);
        }

        @Override
        public void channelReadComplete(ChannelHandlerContext ctx) throws Exception {
            //ctx.writeAndFlush(Unpooled.EMPTY_BUFFER).addListener(ChannelFutureListener.CLOSE);
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
            cause.printStackTrace();
            ctx.close();
        }
    }
    @Override
    public void run() {
        EventLoopGroup group = new NioEventLoopGroup();

        try{
            ServerBootstrap serverBootstrap = new ServerBootstrap();
            serverBootstrap.group(group);
            serverBootstrap.channel(NioServerSocketChannel.class);
            serverBootstrap.localAddress(new InetSocketAddress("0.0.0.0", port));

            serverBootstrap.childHandler(new ChannelInitializer<SocketChannel>() {
                protected void initChannel(SocketChannel socketChannel) throws Exception {
                    socketChannel.pipeline().addLast(new ServerReadHandler());
                }
            });
            ChannelFuture channelFuture = serverBootstrap.bind().sync();
            channelFuture.channel().closeFuture().sync();
        } catch(Exception e){
            e.printStackTrace();
        } finally {
            try {
                group.shutdownGracefully().sync();
            }catch (Exception e){
                e.printStackTrace();
            }
        }
    }

    private static void log(String str) {
        System.out.println(str);
    }

}
