package com.synload.nucleo.socket;

import com.synload.nucleo.NucleoMesh;
import com.synload.nucleo.data.NucleoData;
import com.synload.nucleo.zookeeper.ServiceInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class EManager {
    protected static final Logger logger = LoggerFactory.getLogger(EManager.class);
    NucleoMesh mesh;
    int port;
    HashMap<String, NettyClient> connections = new HashMap<>();
    TreeMap<String, List<NettyClient>> clientConnections = new TreeMap<>();
    TreeMap<String, Thread> connectionThreads = new TreeMap<>();
    HashMap<String, TopicRound> topics = new HashMap<>();
    HashMap<String, NettyClient> leaderTopics = new HashMap<>();
    HashMap<String, String> leaders = new HashMap<>();
    TreeMap<String, List<String>> route = new TreeMap<>();

    public EManager(NucleoMesh mesh, int port){
        this.mesh = mesh;
        this.port = port;
    }
    public void createServer(){
        new Thread(new NettyServer(this.port, this.mesh, this)).start();
    }
    public void leaderCheck(ServiceInformation node){
        if (node.isLeader() &&
            ( leaders.get(node.getService()) != null && !leaders.get(node.getService()).equals(node.getName()) )
            || leaders.get(node.getService()) == null
        ) {
            leaders.put(node.getService(), node.getName());
            if (connections.containsKey(node.getName())) {
                NettyClient eClient = connections.get(node.getName());
                for (String event : node.getEvents()) {
                    leaderTopics.put(event, eClient);
                }
                logger.info(node.getService() + " [ " + node.getName() + " ] : " + node.getConnectString() + " is the leader!");
            }
        }
    }
    public void sync(ServiceInformation node){
        NettyClient nodeClient = null;
        if (!connections.containsKey(node.getName())) {
            logger.info(node.getService() + " : " + node.getConnectString()+ " joined the mesh!");
            nodeClient = new NettyClient(  node, mesh);
            connections.put(node.getName(), nodeClient);
            for (String event : node.getEvents()) {
                synchronized (topics) {
                    if (!topics.containsKey(event)) {
                        topics.put(event, new TopicRound());
                    }
                    topics.get(event).nodes.add(nodeClient);
                    logger.info("Added to [ " + event + " ] from " + node.getHost() + ", nodes available: " + topics.get(event).nodes.size());
                }
            }

        }
    }
    public void delete(String node){
        NettyClient client = null;
        synchronized(connections) {
            if (connections.containsKey(node)) {
                client = connections.remove(node);
                synchronized(connectionThreads) {
                    Thread x = connectionThreads.remove(node);
                    if(x!=null)
                        x.interrupt();
                }
            }
        }
        //logger.debug("connectionThreads: "+connectionThreads.size());
        //logger.debug("connections: "+connections.size());
        if(client!=null){
            logger.info(client.getNode().getService() + " : " + node+ " has left the mesh");
            for (String event : client.getNode().getEvents()) {
                synchronized (topics) {
                    if (topics.containsKey(event)) {
                        topics.get(event).nodes.remove(client);
                        logger.info("Removed event [ " + event + " ] from " + client.getNode().getHost() + ", nodes available: " + topics.get(event).nodes.size());
                        if (topics.get(event).nodes.size() == 0) {
                            logger.info("no nodes on [ " + event + " ], removing");
                            topics.remove(event);
                        }
                    }
                }
            }
            /*client.getQueue().forEach((NucleoTopicPush p) -> {
                this.robin(p.getTopic(), p.getData()); // preserve the queue for this client and send to other clients
            });*/
        }
    }
    public void robin(String topic, NucleoData data){
        //System.out.println(topic);
        if (topic.startsWith("nucleo.client.")) {
            String node = topic.substring(14);
            if (connections.containsKey(node)) {
                connections.get(node).add("nucleo.client." + node, data);
                return;
            }else if(node.equals(mesh.getUniqueName())){
                mesh.getHub().handle(mesh.getHub(), data, topic);
                return;
            }
            /*try{
                System.out.println(new ObjectMapper().writeValueAsString(data));
            }catch (Exception e){}*/
            //System.out.println("[" + topic + "] failed to route on "+mesh.getUniqueName());
        } else {

            if (topics.containsKey(topic)) {
                topics.get(topic).send(topic, data);
                return;
            }
        }
        try {
            //System.out.println("["+topic+"] " + new ObjectMapper().writeValueAsString(connections));
        } catch (Exception e) {
            e.printStackTrace();
        }
        //System.out.println("[" + topic + "] route not found");
    }

    public void leader(String topic, NucleoData data){
        if(leaderTopics.containsKey(topic)){
            NettyClient eClient = leaderTopics.get(topic);
            route(topic, eClient, data);
        }
    }
    public void route(String topic, NettyClient node, NucleoData data){
        synchronized (route) {
            /*List<String> routeToNode = route.get(node.node.name);
            if (routeToNode != null && routeToNode.size() > 0) {
                routeToNode = Lists.newLinkedList(routeToNode);
                String firstNode = routeToNode.remove(0);
                if (routeToNode.size() > 0) {
                    if (connections.containsKey(firstNode)) {
                        synchronized (data) {
                            data.getObjects().put("_route", routeToNode);
                        }
                        connections.get(firstNode).add("nucleo.client." + firstNode, data);
                        data.markTime("Route sending to "+connections.get(firstNode).node.name);
                        return;
                    }
                }
            }*/
            //data.markTime("Route sending to "+node.node.name);
            node.add(topic, data);
            return;
        }
    }

    public class TopicRound{
        public List<NettyClient> nodes = new ArrayList<>();
        public int lastNode=0;
        public synchronized void send(String topic, NucleoData data){
            //System.out.println(topic);
            List<NettyClient> tmpNodes = new ArrayList<>(this.nodes);
            if(lastNode >= tmpNodes.size()){
                lastNode=0;
            }
            if(tmpNodes.size()>0){
                if(tmpNodes.get(lastNode)!=null) {
                    //data.markTime("Robin Done");
                    route(topic, tmpNodes.get(lastNode), data);
                    lastNode++;
                }else{
                    if(tmpNodes.size()==1){
                        System.out.println("No active route found!");
                        return;
                    }
                    lastNode++;
                    loop(topic, data, lastNode-1);
                }
                //tmpNodes.get(lastNode).add(topic, data);

            }
            // just drop any other data with no destination
        }
        public void loop(String topic, NucleoData data, int start){
            //System.out.println(topic);
            List<NettyClient> tmpNodes = new ArrayList<>(this.nodes);
            if(lastNode >= tmpNodes.size()){
                lastNode=0;
            }
            if(start==lastNode){
                logger.debug("No nodes available");
                return;
            }
            if(tmpNodes.size()>0){
                if(tmpNodes.get(lastNode)!=null) {
                    //data.markTime("Robin Done");
                    route(topic, tmpNodes.get(lastNode), data);
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

    public NucleoMesh getMesh() {
        return mesh;
    }

    public void setMesh(NucleoMesh mesh) {
        this.mesh = mesh;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public HashMap<String, NettyClient> getConnections() {
        return connections;
    }

    public void setConnections(HashMap<String, NettyClient> connections) {
        this.connections = connections;
    }

    public HashMap<String, TopicRound> getTopics() {
        return topics;
    }

    public void setTopics(HashMap<String, TopicRound> topics) {
        this.topics = topics;
    }

    public TreeMap<String, List<String>> getRoute() {
        return route;
    }

    public void setRoute(TreeMap<String, List<String>> route) {
        this.route = route;
    }
}
