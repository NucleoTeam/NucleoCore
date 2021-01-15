package com.synload.nucleo.interlink;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.synload.nucleo.data.NucleoData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Stack;
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
        public void send(String topic, NucleoData data){
            //System.out.println(topic);
            List<InterlinkClient> tmpNodes = new ArrayList<>(this.nodes);
            if(lastNode >= tmpNodes.size()){
                lastNode=0;
            }
            if(tmpNodes.size()>0){
                Stack<InterlinkClient> visited = new Stack<>();
                //data.markTime("Robin Done");
                InterlinkClient client = tmpNodes.get(lastNode);
                while(!visited.contains(client)){
                    visited.add(client);
                    if (client.isConnected()) {
                        logger.debug(data.getRoot().toString() + ": Send data to " + client.getServiceInformation().getConnectString());
                        interlinkManager.route(topic, client, data);
                        lastNode++;
                        return;
                    } else {
                        logger.debug(data.getRoot().toString() + ": Connection to " + client.getServiceInformation().getConnectString() + " not connected, trying another connection.");
                        lastNode++;
                    }
                    if(lastNode >= tmpNodes.size()){
                        lastNode=0;
                    }
                    client = tmpNodes.get(lastNode);
                }
                logger.debug("No nodes available");
                //tmpNodes.get(lastNode).add(topic, data);
            }else{
                logger.error(data.getRoot().toString()+ ": No Route to topic ", topic);
            }
            // just drop any other data with no destination
        }
    }
}
