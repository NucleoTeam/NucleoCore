package com.synload.nucleo.socket;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Queues;
import com.synload.nucleo.NucleoMesh;
import com.synload.nucleo.event.NucleoData;
import com.synload.nucleo.zookeeper.ServiceInformation;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.util.CharsetUtil;

import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.net.SocketException;
import java.nio.ByteBuffer;
import java.time.Clock;
import java.time.Instant;
import java.util.Date;
import java.util.HashMap;
import java.util.Queue;

public class ChannelClient implements Runnable {
    public ServiceInformation node;
    public boolean direction;
    public NucleoMesh mesh;
    public int streams = 0;
    public Queue<NucleoTopicPush> queue = Queues.newArrayDeque();
    public ObjectMapper mapper;
    public boolean reconnect = true;

    public static int readSize = 1024;

    public void add(String topic, NucleoData data){
        queue.add(new NucleoTopicPush(topic, data));
    }
    public ChannelClient(ServiceInformation node, NucleoMesh mesh){
        this.node = node;
        this.mesh = mesh;
        this.mapper = new ObjectMapper();
    }
    public synchronized NucleoTopicPush pop(){
        if(!queue.isEmpty()) {
            return queue.remove();
        }
        return null;
    }

    private synchronized void streams(){
        streams++;
    }

    public class ClientHandler extends SimpleChannelInboundHandler {

        @Override
        public void channelActive(ChannelHandlerContext channelHandlerContext){
            System.out.println("Channel started");
            while(true) {
                while (queue.size() > 0) {
                    NucleoTopicPush push = pop();
                    push.getData().markTime("Write to Socket");
                    if (push != null) {
                        try {
                            byte[] data = mapper.writeValueAsBytes(push);
                            //System.out.println("Writing data with length: "+data.length);
                            ByteBuf buff = Unpooled.buffer(data.length+4, data.length+4);
                            buff.writeBytes(ByteBuffer.allocate(4).putInt(data.length).array());
                            buff.writeBytes(data);
                            channelHandlerContext.writeAndFlush(buff);
                            //System.out.println("Wrote: "+data.length);
                            buff.clear();
                        } catch (Exception e) {
                            if (push != null) {
                                mesh.geteManager().robin(push.getTopic(), push.getData());
                            }
                            e.printStackTrace();
                        }
                    }
                }
                try {
                    Thread.sleep(0, 1);
                }catch (Exception e){
                    e.printStackTrace();
                }
            }
        }

        @Override
        protected void channelRead0(ChannelHandlerContext ctx, Object msg) throws Exception {
            System.out.println("data received");
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext channelHandlerContext, Throwable cause){
            System.out.println("ERROR");
            cause.printStackTrace();
            channelHandlerContext.close();
        }
    }

    @Override
    public void run() {
        try {
            while (reconnect && !Thread.currentThread().isInterrupted()) {
                if(node!=null) {
                    System.out.println("Starting new connection to " + node.getConnectString()+ " size:"+queue.size());
                }
                if(node.getConnectString()==null) {
                    return;
                }
                String[] connectArr = node.getConnectString().split(":");
                EventLoopGroup group = new NioEventLoopGroup();
                try {
                    Bootstrap clientBootstrap = new Bootstrap();

                    clientBootstrap.group(group);
                    clientBootstrap.channel(NioSocketChannel.class);
                    clientBootstrap.remoteAddress(new InetSocketAddress(connectArr[0], Integer.valueOf(connectArr[1])));
                    clientBootstrap.handler(new ChannelInitializer<SocketChannel>() {
                        protected void initChannel(SocketChannel socketChannel) throws Exception {
                            socketChannel.pipeline().addLast(new ClientHandler());
                        }
                    });
                    ChannelFuture channelFuture = clientBootstrap.connect().sync();
                    channelFuture.channel().closeFuture().sync();
                }catch (Exception e){

                } finally {
                    group.shutdownGracefully().sync();
                }
                try {
                    Thread.sleep(1, 500);
                }catch (Exception e){
                    e.printStackTrace();
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            node=null;
        }
    }

    public ServiceInformation getNode() {
        return node;
    }

    public void setNode(ServiceInformation node) {
        this.node = node;
    }

    public boolean isDirection() {
        return direction;
    }

    public void setDirection(boolean direction) {
        this.direction = direction;
    }

    public NucleoMesh getMesh() {
        return mesh;
    }

    public void setMesh(NucleoMesh mesh) {
        this.mesh = mesh;
    }

    public Queue<NucleoTopicPush> getQueue() {
        return queue;
    }

    public void setQueue(Queue<NucleoTopicPush> queue) {
        this.queue = queue;
    }

    public ObjectMapper getMapper() {
        return mapper;
    }

    public void setMapper(ObjectMapper mapper) {
        this.mapper = mapper;
    }

    public boolean isReconnect() {
        return reconnect;
    }

    public void setReconnect(boolean reconnect) {
        this.reconnect = reconnect;
    }


}

