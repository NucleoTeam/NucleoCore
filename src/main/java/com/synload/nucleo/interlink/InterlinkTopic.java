package com.synload.nucleo.interlink;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.synload.nucleo.data.NucleoData;
import com.synload.nucleo.interlink.netty.NettyServer;
import com.synload.nucleo.interlink.socket.SocketClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class InterlinkTopic {
    @JsonIgnore
    protected static final Logger logger = LoggerFactory.getLogger(InterlinkTopic.class);

    public static class RoundRobin {
        InterlinkManager interlinkManager;
        public RoundRobin(InterlinkManager interlinkManager){
            this.interlinkManager = interlinkManager;
        }
        public List<InterlinkClient> nodes = new ArrayList<>();
        public int lastNode=0;
        public void add(InterlinkClient client){
            nodes.add(client);
        }
        public synchronized void send(String topic, NucleoData data){
            //System.out.println(topic);
            List<InterlinkClient> tmpNodes = new ArrayList<>(this.nodes).stream().filter(i->i.isConnected()).collect(Collectors.toList());
            if(lastNode >= tmpNodes.size()){
                lastNode=0;
            }
            if(tmpNodes.size()>0){
                //data.markTime("Robin Done");
                interlinkManager.route(topic, tmpNodes.get(lastNode), data);
                lastNode++;
                //tmpNodes.get(lastNode).add(topic, data);
            }else{
                logger.error("No Route to topic ", topic);
            }
            // just drop any other data with no destination
        }
        public void loop(String topic, NucleoData data, int start){
            //System.out.println(topic);
            List<InterlinkClient> tmpNodes = new ArrayList<>(this.nodes).stream().filter(i->i.isConnected()).collect(Collectors.toList());
            if(lastNode >= tmpNodes.size()){
                lastNode=0;
            }
            if(start==lastNode){
                logger.debug("No nodes available");
                return;
            }
            if(tmpNodes.size()>0){
                if(tmpNodes.get(lastNode).isConnected()) {
                    //data.markTime("Robin Done");
                    interlinkManager.route(topic, tmpNodes.get(lastNode), data);
                    lastNode++;
                }else{
                    if(tmpNodes.size()==1){
                        logger.debug("No active route found!");
                        return;
                    }
                    lastNode++;
                    loop(topic, data, start);
                }
                //tmpNodes.get(lastNode).add(topic, data);

            }
            // just drop any other data with no destination
        }
    }
}
