package com.synload.nucleo.socket;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.synload.nucleo.NucleoMesh;
import com.synload.nucleo.event.NucleoData;
import com.synload.nucleo.hub.Hub;
import com.synload.nucleo.zookeeper.ServiceInformation;

import java.io.*;
import java.net.Socket;
import java.util.Stack;

public class EClient implements Runnable {
    public ServiceInformation node;
    public boolean direction;
    public NucleoMesh mesh;
    public Socket client;
    public Stack<NucleoTopicPush> queue = new Stack<>();
    public ObjectMapper mapper;

    public void add(String topic, NucleoData data){
        queue.add(new NucleoTopicPush(topic, data));
    }
    public EClient(Socket client, ServiceInformation node, NucleoMesh mesh){
        this.node = node;
        this.mesh = mesh;
        this.client = client;
        this.direction = ( client != null );
        this.mapper = new ObjectMapper();
    }

    @Override
    public void run() {
        try {
            if(this.direction){
                try {
                    DataInputStream is = new DataInputStream(client.getInputStream());
                    String objRead = "";
                    while(true){
                        if(is.available()>0) {
                            objRead += is.readUTF();
                            String[] arr = objRead.split("§");
                            if (arr.length > 1) {
                                int i = 0;
                                for (i = 0; i < arr.length - 1; i++) {
                                    String[] arrTop = arr[i].split("þ");
                                    NucleoData data = mapper.readValue(arrTop[1], NucleoData.class);
                                    mesh.getHub().handle(mesh.getHub(), data, arrTop[0].trim());
                                }
                            }
                            objRead = arr[arr.length-1];
                            System.out.println(objRead);
                        }
                        Thread.sleep(1L);
                    }
                }catch (Exception e){
                    e.printStackTrace();
                }finally {
                    client.close();
                }
            }else{
                String[] connectArr = node.getConnectString().split(":");
                System.out.println("Connecting to " + connectArr[0] + " on port " + connectArr[1]);
                client = new Socket(connectArr[0], Integer.valueOf(connectArr[1]));
                try{
                    DataOutputStream os = new DataOutputStream(client.getOutputStream());
                    while(true){
                        if(!queue.empty()) {
                            NucleoTopicPush push = queue.pop();
                            os.writeUTF( push.getTopic() + "þ" + mapper.writeValueAsString(push.getData()) + "§ ");
                        }
                        Thread.sleep(1L);
                    }
                }catch (Exception e){
                    e.printStackTrace();
                }finally {
                    client.close();
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
    public class NucleoTopicPush{
        NucleoData data = null;
        String topic = null;
        public NucleoTopicPush(String topic, NucleoData data){
            this.topic = topic;
            this.data = data;
        }
        public NucleoData getData() {
            return data;
        }

        public void setData(NucleoData data) {
            this.data = data;
        }

        public String getTopic() {
            return topic;
        }

        public void setTopic(String topic) {
            this.topic = topic;
        }
    }
}