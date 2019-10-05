package com.synload.nucleo.zookeeper;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.synload.nucleo.NucleoMesh;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.api.BackgroundCallback;
import org.apache.curator.framework.api.CuratorEvent;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;

import java.util.*;

public class ZooKeeperManager implements Runnable {

    private CuratorFramework zooClient;
    private Connection connection;
    private String connString;
    private String meshName;
    private NucleoMesh mesh;

    public ZooKeeperManager(String zkConnectionString, NucleoMesh mesh) {
        try {
            this.connString = zkConnectionString;
            this.meshName = mesh.getMeshName();
            this.mesh = mesh;
            connection = new Connection();
            new Thread(this).start();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    public String getConnString() {
        return connString;
    }

    public void setConnString(String connString) {
        this.connString = connString;
    }

    public Set<String> getNodes() {
        return nodes;
    }

    public void setNodes(Set<String> nodes) {
        this.nodes = nodes;
    }

    public TreeMap<String, NodeStatus> getNodePing() {
        return nodePing;
    }

    public void setNodePing(TreeMap<String, NodeStatus> nodePing) {
        this.nodePing = nodePing;
    }

    public int getShowIndex() {
        return showIndex;
    }

    public void setShowIndex(int showIndex) {
        this.showIndex = showIndex;
    }

    public void run() {
        try {
            System.out.println("Connecting to Zookeeper.");
            zooClient = connection.connect(this.connString);
            System.out.println("UpSet service " + mesh.getServiceName() + " to zookeeper");
            create("/" + meshName, (_____, __) ->
                create("/" + meshName + "/services", (___, ____) ->
                    create("/" + meshName + "/services/" + mesh.getServiceName(), (client, event) -> {
                        System.out.println("=======================================================");
                        System.out.println("/" + meshName + "/services/" + mesh.getServiceName());
                        System.out.println(KeeperException.Code.get(event.getResultCode()));
                        System.out.println("Starting zookeeper sync");
                        new Thread(new SyncList()).start();
                        new Thread(new WatchNodeList()).start();
                        mesh.zookeeperConnected();
                    })
                )
            );
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void delete(String path, BackgroundCallback callback) {
        try {
            zooClient.delete().inBackground(callback).forPath(path);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void create(String path, BackgroundCallback callback) {
        try {
            zooClient.create().withMode(CreateMode.PERSISTENT).inBackground(callback).forPath(path);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void closeConnection() throws Exception {
        connection.close();
    }

    public void getServiceList(DataUpdate responder) {
        try {
            zooClient.getChildren().watched().inBackground((CuratorFramework client, CuratorEvent event) -> {
                List<String> children = event.getChildren();
                responder.run("/" + this.meshName + "/services", children);
            }).forPath("/" + this.meshName + "/services");


        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void getServiceNodeList(String service, DataUpdate responder) {
        try {
            zooClient.getChildren().watched().inBackground((CuratorFramework client, CuratorEvent event) -> {
                List<String> children = event.getChildren();
                responder.run(service, children);
            }).forPath("/" + this.meshName + "/services/" + service);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void getServiceNodeInformation(String service, String node, DataUpdate responder, boolean initial) {
        try {
            zooClient.getData().watched().inBackground((CuratorFramework client, CuratorEvent event) -> {
                byte[] data = event.getData();
                //System.out.println("=======================================================|||");
                //System.out.println("/" + this.meshName + "/services/" + service + "/" + node);
                //System.out.println(KeeperException.Code.get(event.getResultCode()));
                if (data != null) {
                    try {
                        responder.run(service, node, new ObjectMapper().readValue(data, ServiceInformation.class));
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                } else {
                    responder.run(service, node, null);
                }
            }).forPath("/" + this.meshName + "/services/" + service + "/" + node);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void register(String path, byte[] data) {
        try {
            zooClient.create().withMode(CreateMode.EPHEMERAL).inBackground((CuratorFramework client, CuratorEvent event) -> {
                //System.out.println("=======================================================");
                //System.out.println(path);
                //System.out.println(KeeperException.Code.get(event.getResultCode()));
            }).forPath(path, data);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    Set<String> nodes = new HashSet<>();
    TreeMap<String, NodeStatus> nodePing = new TreeMap<>();
    int showIndex = 0;

    public class WatchNodeList implements Runnable {
        public void run() {
            ObjectMapper om = new ObjectMapper();
            while (true) {
                /*for(int i=1;i<=((nodesTMP.size()>2)?2:nodesTMP.size());i++) {
                    for (Set<String> nodeStrsTmp : Sets.combinations(nodesTMP, i)) {
                        for (List<String> nodeStrs : Collections2.permutations(nodeStrsTmp)) {
                            String firstNode = "";
                            String finalNode = "";
                            String tmpKey = "";
                            List<String> nodePingList = Lists.newLinkedList();
                            final List<String> nodePingListTmp = Lists.newLinkedList();
                            for (String nodeStr : nodeStrs) {
                                String[] parts = nodeStr.split(",");
                                if (!tmpKey.equals("")) {
                                    tmpKey += ",";
                                }
                                tmpKey += parts[0];
                                nodePingListTmp.add(parts[0]);
                                if (firstNode.equals("")) {
                                    firstNode = parts[0];
                                }else{
                                    nodePingList.add(parts[0]);
                                }
                                finalNode = parts[0];
                            }
                            final String destinationNode = finalNode;
                            final String key = tmpKey;
                            TreeMap<String, Object> objects = new TreeMap<>();
                            objects.put("_ping", nodePingList);
                            NucleoData nodeData = mesh.getHub().constructNucleoData("", objects);
                            nodeData.setTrack(0);
                            mesh.getHub().getWriter().add(new Object[]{"nucleo.client." + firstNode, nodeData});
                            mesh.getHub().getResponders().put(nodeData.getRoot().toString(), new NucleoResponder() {
                                @Override
                                public void run(NucleoData data) {
                                    try {
                                        System.out.println(om.writeValueAsString(data));
                                    } catch (Exception e) {
                                        e.printStackTrace();
                                    }
                                    synchronized (nodePing) {
                                        if (!nodePing.containsKey(key)) {
                                            nodePing.put(key, new NodeStatus(key, destinationNode, nodePingListTmp));
                                        }
                                        nodePing.get(key).add(data.getExecution().getTotal());
                                    }
                                }
                            });
                        }
                    }
                }*/
                /*
                for (String nodeStrs : nodesTMP) {
                    TreeMap<String, Object> objects = new TreeMap<>();
                    objects.put("_ping", null);
                    NucleoData nodeData = mesh.getHub().constructNucleoData("", objects);
                    nodeData.setTrack(0);
                    mesh.getHub().getWriter().add(new Object[]{"nucleo.client." + nodeStrs, nodeData});
                    mesh.getHub().getResponders().put(nodeData.getRoot().toString(), new NucleoResponder() {
                        @Override
                        public void run(NucleoData data) {
                            synchronized (nodePing) {
                                if (!nodePing.containsKey(nodeStrs)) {
                                    nodePing.put(nodeStrs, new NodeStatus(nodeStrs, nodeStrs, new Stack<String>() {{
                                        add(nodeStrs);
                                    }}));
                                }
                                nodePing.get(nodeStrs).add(data.getExecution().getTotal());
                            }
                        }
                    });
                }
                synchronized (nodePing) {
                    Calendar calendar = Calendar.getInstance();
                    calendar.add(Calendar.SECOND, -10);
                    for (Map.Entry<String, NodeStatus> status : Maps.newTreeMap(nodePing).entrySet()) {
                        if (calendar.getTime().compareTo(status.getValue().lastPing) > 0) {
                            nodePing.remove(status.getKey());
                            mesh.geteManager().getRoute().remove(status.getValue().connection);
                        }
                    }
                }*/
                if (showIndex > 3) {
                    /*Map<String, Float> fastestRoute = Maps.newLinkedHashMap();
                    synchronized (nodePing) {
                        for (Map.Entry<String, NodeStatus> status : Maps.newTreeMap(nodePing).entrySet()) {
                            if (status.getValue().pings.size() > 5) {
                                List<Long> tmp = Lists.newArrayList(status.getValue().pings);
                                float average = (float) tmp.stream().mapToLong(Long::longValue).sum() / (float) tmp.size();
                                if (fastestRoute.containsKey(status.getValue().connection)) {
                                    float currentAvg = fastestRoute.get(status.getValue().connection);
                                    if (average < currentAvg || (average == currentAvg && mesh.geteManager().getRoute().get(status.getValue().connection).size() > status.getValue().route.size())) {
                                        fastestRoute.put(status.getValue().connection, average);
                                        mesh.geteManager().getRoute().put(status.getValue().connection, status.getValue().route);
                                    }
                                } else {
                                    fastestRoute.put(status.getValue().connection, average);
                                    mesh.geteManager().getRoute().put(status.getValue().connection, status.getValue().route);
                                }
                            }
                        }
                    }
                    try {
                        System.out.println(om.writeValueAsString(mesh.geteManager().getRoute()));
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                    try {
                        System.out.println(om.writeValueAsString(nodePing));
                    } catch (Exception e) {
                        e.printStackTrace();
                    }*/
                    Set<String> nodesTMP = new HashSet<>(nodes);
                    for (String nodeStr : nodesTMP) {
                        //System.out.println("Checking node "+nodeStr);
                        String[] parts = nodeStr.split(",");
                        getServiceNodeInformation(parts[1], parts[0], new DataUpdate() {
                            @Override
                            public void run(String service, String node, ServiceInformation data) {
                                if (data != null) {
                                    mesh.geteManager().sync(data);
                                } else {
                                    mesh.geteManager().delete(node);
                                    nodes.remove(nodeStr);
                                }
                            }
                        }, true);
                    }
                    showIndex = 0;
                }
                showIndex++;
                try {
                    Thread.sleep(2000);
                } catch (Exception e) {

                }
            }
        }
    }

    public class SyncList implements Runnable {
        public void run() {
            while (true) {
                getServiceList(new DataUpdate() {
                    @Override
                    public void run(String path, List<String> registeredServices) {
                        if (registeredServices != null) {
                            for (String service : registeredServices) {
                                getServiceNodeList(service, new DataUpdate() {
                                    @Override
                                    public void run(String service, List<String> serviceNodes) {
                                        for (String node : serviceNodes) {
                                            boolean exists = false;
                                            synchronized (nodes) {
                                                exists = nodes.contains(node + "," + service);
                                            }
                                            if (!exists && !mesh.getUniqueName().equals(node)) {
                                                synchronized (nodes) {
                                                    nodes.add(node + "," + service);
                                                }
                                            }
                                        }
                                    }
                                });
                            }
                        }
                    }
                });
                try {
                    Thread.sleep(5000);
                } catch (Exception e) {

                }
            }
        }
    }
}
