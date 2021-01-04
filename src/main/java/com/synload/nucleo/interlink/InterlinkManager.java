package com.synload.nucleo.interlink;

import com.synload.nucleo.NucleoMesh;
import com.synload.nucleo.data.NucleoData;
import com.synload.nucleo.interlink.socket.SocketServer;
import com.synload.nucleo.zookeeper.ServiceInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationTargetException;
import java.util.*;

public class InterlinkManager {
    protected static final Logger logger = LoggerFactory.getLogger(InterlinkManager.class);
    NucleoMesh mesh;
    int port;
    HashMap<String, InterlinkClient> connections = new HashMap<>();
    TreeMap<String, Thread> connectionThreads = new TreeMap<>();
    HashMap<String, InterlinkTopic.RoundRobin> topics = new HashMap<>();
    HashMap<String, InterlinkClient> leaderTopics = new HashMap<>();
    HashMap<String, String> leaders = new HashMap<>();
    TreeMap<String, List<String>> route = new TreeMap<>();
    Class serverClass;
    Class clientClass;

    public InterlinkManager(NucleoMesh mesh, int port, Class serverClass, Class clientClass){
        this.mesh = mesh;
        this.port = port;
        this.serverClass = serverClass;
        this.clientClass = clientClass;
    }
    public void createServer() {
        new Thread(new SocketServer(this.port, this.mesh, this)).start();
        serverClass.getDeclaredConstructor()
        mesh.getHub().handle(mesh.getHub(), data.getData(), data.getTopic());
    }
    public void leaderCheck(ServiceInformation node){
        if (node.isLeader() &&
            ( leaders.get(node.getService()) != null && !leaders.get(node.getService()).equals(node.getName()) )
            || leaders.get(node.getService()) == null
        ) {
            leaders.put(node.getService(), node.getName());
            if (connections.containsKey(node.getName())) {
                InterlinkClient socketClient = connections.get(node.getName());
                for (String event : node.getEvents()) {
                    leaderTopics.put(event, socketClient);
                }
                logger.info(node.getService() + " [ " + node.getName() + " ] : " + node.getConnectString() + " is the leader!");
            }
        }
    }
    public void sync(ServiceInformation node) {
        InterlinkClient nodeClient = null;
        if (!connections.containsKey(node.getName())) {
            logger.info(node.getService() + " : " + node.getConnectString()+ " joined the mesh!");

            try {
                nodeClient = (InterlinkClient)clientClass.getDeclaredConstructor(ServiceInformation.class, InterlinkHandler.class).newInstance(node, (InterlinkHandler)(topic, data)->{
                    route(topic, data); // if sending fails use route again
                });
            }catch (NoSuchMethodException e){
                logger.error("Interlink client class constructor not defined.");
                e.printStackTrace();
            } catch (IllegalAccessException e) {
                e.printStackTrace();
            } catch (InstantiationException e) {
                e.printStackTrace();
            } catch (InvocationTargetException e) {
                e.printStackTrace();
            }

            connections.put(node.getName(), nodeClient);
            try {
                Thread thread = new Thread(nodeClient);
                synchronized(connectionThreads) {
                    connectionThreads.put(node.getName(), thread);
                }
                thread.start();
            } catch (Exception e) {

            }
            for (String event : node.getEvents()) {
                synchronized (topics) {
                    if (!topics.containsKey(event)) {
                        topics.put(event, new InterlinkTopic.RoundRobin(this)); // use round robin
                    }
                    topics.get(event).add(nodeClient);
                    logger.info("Added to [ " + event + " ] from " + node.getHost() + ", nodes available: " + topics.get(event).nodes.size());
                }
            }

        }
    }
    public void delete(String node){
        InterlinkClient client = null;
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
            logger.info(client.getServiceInformation().getService() + " : " + node+ " has left the mesh");
            for (String event : client.getServiceInformation().getEvents()) {
                synchronized (topics) {
                    if (topics.containsKey(event)) {
                        topics.get(event).nodes.remove(client);
                        logger.info("Removed event [ " + event + " ] from " + client.getServiceInformation().getHost() + ", nodes available: " + topics.get(event).nodes.size());
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
    public void route(String topic, NucleoData data){
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
            InterlinkClient leaderWriteConnection = leaderTopics.get(topic);
            route(topic, leaderWriteConnection, data);
        }
    }
    public void route(String topic, InterlinkClient node, NucleoData data) {
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

    public HashMap<String, InterlinkClient> getConnections() {
        return connections;
    }

    public void setConnections(HashMap<String, InterlinkClient> connections) {
        this.connections = connections;
    }

    public HashMap<String, InterlinkTopic.RoundRobin> getTopics() {
        return topics;
    }

    public void setTopics(HashMap<String, InterlinkTopic.RoundRobin> topics) {
        this.topics = topics;
    }

    public TreeMap<String, List<String>> getRoute() {
        return route;
    }

    public void setRoute(TreeMap<String, List<String>> route) {
        this.route = route;
    }
}
