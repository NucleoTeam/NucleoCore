package com.synload.nucleo;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.synload.nucleo.event.NucleoData;
import com.synload.nucleo.event.NucleoResponder;
import com.synload.nucleo.hub.Hub;
import com.synload.nucleo.information.HitsHandler;
import com.synload.nucleo.information.InformationHandler;
import com.synload.nucleo.loader.LoadHandler;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import java.lang.reflect.Method;
import java.util.TreeMap;
import java.util.UUID;


public class NucleoMesh {
  private Hub hub;
  private String clientName;

  public NucleoMesh( String clientName, String bootstrapServer, String groupName) {
    hub = new Hub(bootstrapServer, clientName, groupName);
    this.clientName = clientName;
  }

  public void call(String chain, TreeMap<String, Object> objects, Method onFinishedMethod, Object onFinishedObject){
    NucleoData data = new NucleoData();
    data.setObjects(objects);
    data.setOrigin(clientName);
    data.setLink(0);
    data.setChain(chain.split("\\."));
    this.getHub().push(data, new NucleoResponder(){
      @Override
      public void run(NucleoData returnedData){
        try {
          onFinishedMethod.invoke(onFinishedObject, returnedData);
        }catch (Exception e){
          e.printStackTrace();
        }
      }
    });
  }
  public void call(String chain, TreeMap<String, Object> objects, NucleoResponder nucleoResponder){
    NucleoData data = new NucleoData();
    data.setObjects(objects);
    data.setOrigin(clientName);
    data.setLink(0);
    data.setChain(chain.split("\\."));
    this.getHub().push(data, nucleoResponder);
  }
  public boolean call(String[] chains, TreeMap<String, Object> objects, NucleoResponder nucleoResponder){
    if(chains.length==0){
      return false;
    }
    objects.put("_onChain", 0);
    call(chains[0], objects, new NucleoResponder(){
      @Override
      public void run(NucleoData data) {
        int onChain = (int)data.getObjects().get("_onChain")+1;
        if(chains.length==onChain){
          nucleoResponder.run(data);
        }else{
          try {
            System.out.println("Chained: " + new ObjectMapper().writeValueAsString(data));
          }catch (Exception e){
            e.printStackTrace();
          }
          data.getObjects().put("_onChain", onChain);
          call(chains[onChain], data.getObjects(), this);
        }
      }
    });
    return true;
  }

  public Hub getHub() {
    return hub;
}

  public static void main(String[] args){
    //createTopic();
    Logger.getRootLogger().setLevel(Level.DEBUG);
    NucleoMesh mesh = new NucleoMesh( "root","192.168.1.170:9092", "mesh");
    mesh.getHub().register(InformationHandler.class, HitsHandler.class);
    mesh.getHub().run();
    while(mesh.getHub().isReady()) {
      try {
        Thread.sleep(1L);
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
    mesh.call(new String[]{"information.hits", "information.changeme"},
      new TreeMap<String, Object>(){{
        put("wow", "works?");
      }},
      new NucleoResponder(){
        @Override
        public void run(NucleoData data) {
          try {
            System.out.println("FINAL "+new ObjectMapper().writeValueAsString(data));
          }catch (Exception e){
            e.printStackTrace();
          }
        }});
  }
}
