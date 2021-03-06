package com.synload.nucleo.examples;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.synload.nucleo.NucleoMesh;
import com.synload.nucleo.data.NucleoData;
import com.synload.nucleo.data.NucleoObject;
import com.synload.nucleo.utils.NucleoDataStats;
import org.apache.curator.shaded.com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class Service {
    protected static final Logger logger = LoggerFactory.getLogger(Service.class);
    @JsonIgnore
    private static ObjectMapper mapper = new ObjectMapper(){{
        this.enableDefaultTyping();
        this.setSerializationInclusion(JsonInclude.Include.NON_NULL);
    }};

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
        NucleoMesh mesh = new NucleoMesh("nucleoTest", "nucleoMesh", "10.0.0.42.:2181",  "10.0.0.42:9092", "com.synload.nucleo.examples");
        new Thread(()->{
            while (!Thread.currentThread().isInterrupted()) {
                mesh.call(
                    new String[]{"information", "[popcorn/information.hits/information.test/information.popcorn]", "information.test", "[popcorn.poppyx/information.hits/information.test]"},
                    new NucleoObject() {{
                        createOrUpdate("wow", "works?");
                        createOrUpdate("time", System.currentTimeMillis());
                    }},
                    (data)->{
                        logger.info("complete in ");
                        Object totalTimeObject =data.getObjects().get("time");
                        if(totalTimeObject!=null) {
                            long totalTime = (System.currentTimeMillis() - ((Long) totalTimeObject).longValue());
                            if (totalTime > 50) {
                                logger.info("timeout for: " + data.getRoot());
                                logger.info("total: " + totalTime + "ms");
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
                    Thread.sleep(200);
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
