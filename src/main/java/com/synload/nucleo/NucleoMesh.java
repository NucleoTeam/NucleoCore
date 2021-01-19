package com.synload.nucleo;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.synload.nucleo.data.NucleoData;
import com.synload.nucleo.data.NucleoObject;
import com.synload.nucleo.event.NucleoResponder;
import com.synload.nucleo.hub.Hub;
import com.synload.nucleo.interlink.InterlinkManager;
import com.synload.nucleo.utils.NucleoDataStats;
import com.synload.nucleo.zookeeper.ZooKeeperManager;
import org.apache.curator.shaded.com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Serializable;
import java.lang.reflect.Method;
import java.net.DatagramSocket;
import java.net.ServerSocket;
import java.util.*;


public class NucleoMesh {
    private Hub hub;
    private String uniqueName;
    private ZooKeeperManager manager;
    private String meshName;
    private String serviceName;
    private InterlinkManager interlinkManager;
    protected static final Logger logger = LoggerFactory.getLogger(NucleoMesh.class);
    @JsonIgnore
    private static ObjectMapper mapper = new ObjectMapper(){{
        this.enableDefaultTyping();
        this.setSerializationInclusion(JsonInclude.Include.NON_NULL);
    }};

    public NucleoMesh(String meshName, String serviceName, String zookeeper, String packageStr) throws ClassNotFoundException {
        this.uniqueName = UUID.randomUUID().toString();
        hub = new Hub(this, uniqueName);
        this.meshName = meshName;
        this.serviceName = serviceName;
        logger.info("Starting nucleo client and joining mesh " + meshName + " with service name " + serviceName);
        int ePort = nextAvailable();
        logger.info("Selected Port: " + ePort);
        this.interlinkManager = new InterlinkManager(
            this,
            ePort,
            Class.forName("com.synload.nucleo.interlink.socket.SocketServer"),
            Class.forName("com.synload.nucleo.interlink.socket.SocketWriteClient")
        );
        interlinkManager.createServer();
        getHub().register(packageStr);
        try {
            manager = new ZooKeeperManager(zookeeper, this);
            manager.create();
            manager.start();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    public void shutdown() throws IOException {
        // close down all connections / threads
        manager.close();
        interlinkManager.close();
        getHub().close();
    }

    public static int nextAvailable() {
        int port = (int) Math.round(Math.random() * 1000) + 9000;
        if (port < 9000 || port > 10000) {
            throw new IllegalArgumentException("Invalid start port: " + port);
        }
        ServerSocket ss = null;
        DatagramSocket ds = null;
        try {
            ss = new ServerSocket(port);
            ss.setReuseAddress(true);
            ds = new DatagramSocket(port);
            ds.setReuseAddress(true);
            return port;
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (ds != null) {
                ds.close();
            }

            if (ss != null) {
                try {
                    ss.close();
                } catch (IOException e) {
                    /* should not be thrown */
                }
            }
        }

        return nextAvailable();
    }


    public void call(String chain, NucleoObject objects, NucleoResponder nucleoResponder) {
        this.getHub().push(hub.constructNucleoData(chain, objects), nucleoResponder, true);
    }

    public boolean call(String[] chains, NucleoObject objects, NucleoResponder nucleoResponder) {
        if (chains.length == 0) {
            return false;
        }
        this.getHub().push(hub.constructNucleoData(chains, objects), nucleoResponder, true);
        return true;
    }

    public Hub getHub() {
        return hub;
    }

    public void setHub(Hub hub) {
        this.hub = hub;
    }

    public String getUniqueName() {
        return uniqueName;
    }

    public void setUniqueName(String uniqueName) {
        this.uniqueName = uniqueName;
    }

    public String getMeshName() {
        return meshName;
    }

    public void setMeshName(String meshName) {
        this.meshName = meshName;
    }

    public String getServiceName() {
        return serviceName;
    }

    public void setServiceName(String serviceName) {
        this.serviceName = serviceName;
    }

    public InterlinkManager getInterlinkManager() {
        return interlinkManager;
    }

    public void setInterlinkManager(InterlinkManager interlinkManager) {
        this.interlinkManager = interlinkManager;
    }

    public ZooKeeperManager getManager() {
        return manager;
    }

    public void setManager(ZooKeeperManager manager) {
        this.manager = manager;
    }

    public static class Test implements Serializable {
        public Test(){

        }
        private String test;
        private List list = new ArrayList();
        private Test pass;

        public String getTest() {
            return test;
        }

        public void setTest(String test) {
            this.test = test;
        }

        public Test getPass() {
            return pass;
        }

        public void setPass(Test pass) {
            this.pass = pass;
        }

        public List getList() {
            return list;
        }

        public void setList(List list) {
            this.list = list;
        }
    }
    public static void main(String[] args) throws ClassNotFoundException {
        mapper.enableDefaultTyping();
        //createTopic();

        Test test = new Test();
        test.setTest("poppy");
        NucleoData data2 = new NucleoData();
        data2.getObjects().createOrUpdate("hello", test);
        if(data2.getObjects().exists("hello.list")) {
            data2.getObjects().addToList("hello.list", "test");
            data2.getObjects().delete("hello.list.[0]");
            data2.getObjects().addToList("hello.list", "works");
        }
        try {
            //logger.info(new ObjectMapper().writeValueAsString(data.getDifferences()));
            logger.info(mapper.writeValueAsString(data2.getObjects()));
        }catch (Exception e){
            e.printStackTrace();
        }
        data2 = new NucleoData();
        test = new Test();

        data2.getObjects().createOrUpdate("hello", test);
        if(data2.getObjects().exists("hello.list")) {
            data2.getObjects().addToList("hello.list", "test");
            data2.getObjects().delete("hello.list.[^,test,works]");

            data2.getObjects().addToList("hello.list", "This is a test");
            data2.getObjects().update("hello.list", Lists.newArrayList());
            data2.getObjects().setLedgerMode(true);
            data2.getObjects().addToList("hello.list", "works");
            System.out.println(data2.getObjects().get("hello.list.[0]"));
            System.out.println((((Test)data2.getObjects().getCurrentObjects().get("hello")).getList()).get(0));
        }
        try {
            //logger.info(new ObjectMapper().writeValueAsString(data.getDifferences()));
            logger.info(mapper.writeValueAsString(data2.getObjects()));
        }catch (Exception e){
            e.printStackTrace();
        }
        NucleoMesh mesh = new NucleoMesh("nucleoTest", "nucleoMesh", "192.168.1.141:2181",  "com.synload.nucleo.information");
        new Thread(()->{
            while (!Thread.currentThread().isInterrupted()) {
                mesh.call(
                    new String[]{"information", "[popcorn/information.hits/information.test/information.popcorn]", "information.test", "[popcorn.poppyx/information.hits/information.test]"},
                    new NucleoObject() {{
                        createOrUpdate("wow", "works?");
                        createOrUpdate("time", System.currentTimeMillis());
                    }},
                    (data)->{
                            Object totalTimeObject =data.getObjects().get("time");
                            if(totalTimeObject!=null) {
                                long totalTime = (System.currentTimeMillis() - ((Long) totalTimeObject).longValue());
                                if (totalTime > 50) {
                                    logger.debug("timeout for: " + data.getRoot());
                                    logger.debug("total: " + totalTime + "ms");
                                } else {
                                    NucleoDataStats stats = new NucleoDataStats();
                                    stats.calculate(data);
                                    if (stats.getAverage() > 0) {
                                        try {
                                            logger.info(new ObjectMapper().writeValueAsString(stats));
                                        } catch (JsonProcessingException e) {
                                            e.printStackTrace();
                                        }
                                    }
                                    logger.info("total: " + totalTime + "ms");
                                    logger.info("data: " + data.getRoot());
                                }
                            }
                    }
                );
                try {
                    Thread.sleep(30000);
                } catch (Exception e) {
                }
            }
        }).start();
        try {
            System.out.println("Press enter/return to quit\n");
            new BufferedReader(new InputStreamReader(System.in)).readLine();
        } catch (IOException e) {
            e.printStackTrace();
        }finally {
            try {
                mesh.shutdown();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        System.exit(1);
    }
}
