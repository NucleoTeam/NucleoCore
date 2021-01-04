package com.synload.nucleo;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.synload.nucleo.data.NucleoData;
import com.synload.nucleo.data.NucleoObject;
import com.synload.nucleo.data.NucleoObjectList;
import com.synload.nucleo.event.NucleoResponder;
import com.synload.nucleo.hub.Hub;
import com.synload.nucleo.interlink.InterlinkManager;
import com.synload.nucleo.zookeeper.ZooKeeperManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
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

    public NucleoMesh(String meshName, String serviceName, String zookeeper, String elasticServer, int elasticPort, String packageStr) {
        this.uniqueName = UUID.randomUUID().toString();
        hub = new Hub(this, uniqueName, elasticServer, elasticPort);
        this.meshName = meshName;
        this.serviceName = serviceName;
        logger.info("Starting nucleo client and joining mesh " + meshName + " with service name " + serviceName);
        int ePort = nextAvailable();
        logger.info("Selected Port: " + ePort);
        this.interlinkManager = new InterlinkManager(this, ePort);
        this.interlinkManager.createServer();
        getHub().register(packageStr);
        try {
            manager = new ZooKeeperManager(zookeeper, this);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static int nextAvailable() {
        int port = (int) Math.round(Math.random() * 1000) + 8000;
        if (port < 8000 || port > 9000) {
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

    public void call(String chain, NucleoObject objects, Method onFinishedMethod, Object onFinishedObject) {
        this.getHub().push(hub.constructNucleoData(chain, objects), new NucleoResponder() {
            @Override
            public void run(NucleoData returnedData) {
                try {
                    onFinishedMethod.invoke(onFinishedObject, returnedData);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }, true);
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

    public static class Test{
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
    public static void main(String[] args) {
        mapper.enableDefaultTyping();
        //createTopic();

        Test test = new Test();
        test.setTest("poppy");
        NucleoData data = new NucleoData();
        data.latestObjects();
        data.getObjects().set("hello", test);
        NucleoObjectList list = data.getObjects().list("hello.list");
        if(list!=null)
            list.add("test");
        list.delete(0);
        if(list!=null)
            list.add("works");
        try {
            //logger.info(new ObjectMapper().writeValueAsString(data.getDifferences()));
            logger.info(mapper.writeValueAsString(data.latestObjects()));

        }catch (Exception e){
            e.printStackTrace();
        }
        NucleoMesh mesh = new NucleoMesh("mcbans", "nucleocore", "192.168.1.7:2181", "192.168.1.7", 9200, "com.synload.nucleo.information");
        while (true) {
            mesh.call(
                new String[]{"information", "[popcorn/information.hits/information.test/information.popcorn]", "information.test", "[popcorn.poppyx/information.hits/information.test]"},
                new NucleoObject() {{
                    set("wow", "works?");
                    set("time", System.currentTimeMillis());
                }},
                new NucleoResponder() {
                    @Override
                    public void run(NucleoData data) {
                        long totalTime = (System.currentTimeMillis() - (long) data.latestObjects().get("time"));
                        if (totalTime > 50) {
                            logger.info("timeout for: " + data.getRoot());
                            logger.debug("total: " + totalTime + "ms");
                        } else {
                            logger.info("total: " + totalTime + "ms");
                            logger.info("data: " + data.getRoot());
                        }
                    }
                }
            );
            try {
                Thread.sleep(2000);
            } catch (Exception e) {
            }
        }

    }
}
