package com.synload.nucleo.socket;

import com.synload.nucleo.NucleoMesh;
import com.synload.nucleo.event.NucleoData;
import com.synload.nucleo.zookeeper.ServiceInformation;

import java.util.ArrayList;
import java.util.List;
import java.util.Stack;
import java.util.TreeMap;

public class EManager {
    NucleoMesh mesh;
    int port;
    TreeMap<String, EClient> connections = new TreeMap<>();
    TreeMap<String, TopicRound> topics = new TreeMap<>();
    public EManager(NucleoMesh mesh, int port){
        this.mesh = mesh;
        this.port = port;
    }
    public void createServer(){
        new Thread(new EServer(this.port, this.mesh)).start();
    }
    public void sync(ServiceInformation node){
        EClient nodeClient = null;
        if (!connections.containsKey(node.getName())) {
            nodeClient = new EClient(null, node, mesh);
            connections.put(node.getName(), nodeClient);
        }
        if(nodeClient!=null){
            try {
                new Thread(nodeClient).start();
            } catch (Exception e) {

            }

            for (String event : node.getEvents()) {
                System.out.println(nodeClient.getNode().getConnectString() + " <- " + event);
                synchronized (topics) {
                    if (!topics.containsKey(event)) {
                        topics.put(event, new TopicRound());
                    }
                    topics.get(event).nodes.add(nodeClient);
                }
            }

        }
    }
    public void delete(String node){
        EClient client = null;
        synchronized(connections) {
            if (connections.containsKey(node)) {
                client = connections.remove(node);
            }
        }
        if(client!=null){
            client.setReconnect(false);
            try {
                if (client.getClient() != null)
                    client.getClient().close();
            } catch (Exception e) {
                e.printStackTrace();
            }
            for (String event : client.getNode().getEvents()) {
                if (topics.containsKey(event)) {
                    synchronized (topics) {
                        topics.get(event).nodes.remove(client);
                    }
                    System.out.println("Removed from [ " + event + " ], nodes left: " + topics.get(event).nodes.size());
                    if (topics.get(event).nodes.size() == 0) {
                        System.out.println("no nodes on [ " + event + " ], removing");
                        synchronized (topics) {
                            topics.remove(event);
                        }
                    }
                }
            }
        }
    }
    public void robin(String topic, NucleoData data){
        synchronized (topics) {
            if (topics.containsKey(topic)) {
                topics.get(topic).send(topic, data);
            } else if (topic.startsWith("nucleo.client.")) {
                String node = topic.substring(14);
                if (connections.containsKey(node)) {
                    connections.get(node).add(topic, data);
                } else {
                    System.out.println("[" + node + "] connection not found");
                }
            } else {
                //System.out.println("[" + topic + "] route not found");
            }
        }
    }

    public class TopicRound{
        public List<EClient> nodes = new ArrayList<>();
        public int lastNode=0;
        public void send(String topic, NucleoData data){
            List<EClient> tmpNodes = new ArrayList<>(this.nodes);
            if(tmpNodes.size()>=lastNode){
                lastNode=0;
            }
            EClient ec = tmpNodes.get(lastNode);
            ec.add(topic, data);
            lastNode++;
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
