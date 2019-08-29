package com.synload.nucleo.socket;

import com.synload.nucleo.NucleoMesh;
import com.synload.nucleo.event.NucleoData;
import com.synload.nucleo.zookeeper.ZooKeeperManager;
import com.synload.nucleo.zookeeper.ServiceInformation;

import java.util.ArrayList;
import java.util.List;
import java.util.TreeMap;

public class EManager {
    NucleoMesh mesh;
    int port;
    TreeMap<String, EClient> connections = new TreeMap<>();
    TreeMap<String, List<EClient>> clientConnections = new TreeMap<>();
    TreeMap<String, Thread> connectionThreads = new TreeMap<>();
    TreeMap<String, TopicRound> topics = new TreeMap<>();
    public EManager(NucleoMesh mesh, int port){
        this.mesh = mesh;
        this.port = port;
    }
    public void createServer(){
        new Thread(new EServer(this.port, this.mesh, this)).start();
    }
    public void sync(ServiceInformation node){
        EClient nodeClient = null;
        if (!connections.containsKey(node.getName())) {
            System.out.println(node.getService() + " : " + node.getConnectString()+ " joined the mesh!");
            nodeClient = new EClient(null, node, mesh);
            connections.put(node.getName(), nodeClient);
        }
        if(nodeClient!=null){
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
                        topics.put(event, new TopicRound());
                    }
                    topics.get(event).nodes.add(nodeClient);
                    //System.out.println("Added to [ " + event + " ], nodes available: " + topics.get(event).nodes.size());
                }
            }

        }
    }
    public void delete(String node){
        EClient client = null;
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
        //System.out.println("connectionThreads: "+connectionThreads.size());
        //System.out.println("connections: "+connections.size());
        if(client!=null){
            client.setReconnect(false);
            try {
                if (client.getClient() != null)
                    client.getClient().close();
            } catch (Exception e) {
                e.printStackTrace();
            }
            System.out.println(client.getNode().getService() + " : " + node+ " has left the mesh");
            for (String event : client.getNode().getEvents()) {
                synchronized (topics) {
                    if (topics.containsKey(event)) {
                        topics.get(event).nodes.remove(client);
                        //System.out.println("Removed from [ " + event + " ], nodes available: " + topics.get(event).nodes.size());
                        if (topics.get(event).nodes.size() == 0) {
                            //System.out.println("no nodes on [ " + event + " ], removing");
                            topics.remove(event);
                        }
                    }
                }
            }
            client.getQueue().forEach((NucleoTopicPush p) -> {
                this.robin(p.getTopic(), p.getData()); // preserve the queue for this client and send to other clients
            });
        }
    }
    public void robin(String topic, NucleoData data){
        if (topics.containsKey(topic)) {
            topics.get(topic).send(topic, data);
        }
        if (topic.startsWith("nucleo.client.")) {
            String node = topic.substring(14);
            if (connections.containsKey(node)) {
                connections.get(node).add(topic, data);
            } else {
                //System.out.println("[" + node + "] missing, ignoring broken chain");
            }
        } else {
            //System.out.println("[" + topic + "] route not found");
        }
    }

    public class TopicRound{
        public List<EClient> nodes = new ArrayList<>();
        public int lastNode=0;
        public void send(String topic, NucleoData data){
            List<EClient> tmpNodes = new ArrayList<>(this.nodes);
            if(lastNode >= tmpNodes.size()){
                lastNode=0;
            }
            if(tmpNodes.size()>0){
                tmpNodes.get(lastNode).add(topic, data);
                lastNode++;
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

    public TreeMap<String, EClient> getConnections() {
        return connections;
    }

    public void setConnections(TreeMap<String, EClient> connections) {
        this.connections = connections;
    }

    public TreeMap<String, TopicRound> getTopics() {
        return topics;
    }

    public void setTopics(TreeMap<String, TopicRound> topics) {
        this.topics = topics;
    }
}
