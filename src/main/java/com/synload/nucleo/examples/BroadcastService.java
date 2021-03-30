package com.synload.nucleo.examples;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mxgraph.util.mxConstants;
import com.synload.nucleo.NucleoMesh;
import com.synload.nucleo.chain.path.PathBuilder;
import com.synload.nucleo.chain.path.PathGenerationException;
import com.synload.nucleo.data.NucleoData;
import com.synload.nucleo.data.NucleoObject;
import com.synload.nucleo.utils.SwingPathDisplay;
import org.apache.curator.shaded.com.google.common.collect.Lists;
import org.jgrapht.ext.JGraphXAdapter;
import org.jgrapht.graph.DefaultEdge;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class BroadcastService {
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
        private Service.Test pass;

        public String getTest() {
            return test;
        }

        public void setTest(String test) {
            this.test = test;
        }

        public Service.Test getPass() {
            return pass;
        }

        public void setPass(Service.Test pass) {
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
        PathBuilder pb = null;
        try {
            NucleoMesh mesh = new NucleoMesh("nucleoTest", "nucleoMesh", "10.0.0.37.:2181",  "10.0.0.37:9092", "com.synload.nucleo.examples");
            try {
                Thread.sleep(5000);
            } catch (Exception e) {
            }
            pb = new PathBuilder()
                .addParallel(
                    PathBuilder.generateRun("forum.list"),
                    PathBuilder.generateRun("adminpanel.statistics")
                );

            SwingPathDisplay.display("Path Builder chain path display.", pb.display());
            pb.addRequirements(mesh.getNucleoMetaHandler());
            SwingPathDisplay.display("Path Builder chain path display. [REQUIREMENTS]", pb.display());
            pb.optimize(mesh.getNucleoMetaHandler());
            SwingPathDisplay.display("Path Builder chain path display. [OPTIMIZED] ", pb.display());

        } catch (PathGenerationException | ClassNotFoundException e) {
            e.printStackTrace();
        }
    }
}
