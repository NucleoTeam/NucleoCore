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

  public NucleoMesh(String clientName, String bootstrapServer, String groupName) {
    hub = new Hub(clientName, bootstrapServer, groupName);
    this.clientName = hub.getClientName();
  }
  public NucleoData constructNucleoData(String chain, TreeMap<String, Object> objects){
    NucleoData data = new NucleoData();
    data.setObjects(objects);
    data.setOrigin(clientName);
    data.setLink(0);
    data.setOnChain(0);
    data.getChainList().add(chain.split("\\."));
    return data;
  }
  public NucleoData constructNucleoData(String[] chains, TreeMap<String, Object> objects){
    NucleoData data = new NucleoData();
    data.setObjects(objects);
    data.setOrigin(clientName);
    data.setLink(0);
    data.setOnChain(0);
    for(String chain : chains) {
      data.getChainList().add(chain.split("\\."));
    }
    return data;
  }
  public void call(String chain, TreeMap<String, Object> objects, Method onFinishedMethod, Object onFinishedObject) {
    this.getHub().push(this.constructNucleoData(chain, objects), new NucleoResponder() {
      @Override
      public void run(NucleoData returnedData) {
        try {
          onFinishedMethod.invoke(onFinishedObject, returnedData);
        } catch (Exception e) {
          e.printStackTrace();
        }
      }
    });
  }
  public void call(String chain, TreeMap<String, Object> objects, NucleoResponder nucleoResponder) {
    this.getHub().push(this.constructNucleoData(chain, objects), nucleoResponder);
  }
  public boolean call(String[] chains, TreeMap<String, Object> objects, NucleoResponder nucleoResponder) {
    if (chains.length == 0) {
      return false;
    }
    this.getHub().push( constructNucleoData(chains, objects), nucleoResponder);
    return true;
  }

  public Hub getHub() {
    return hub;
  }

  public static void main(String[] args) {
    //createTopic();
    Logger.getRootLogger().setLevel(Level.DEBUG);
    NucleoMesh mesh = new NucleoMesh( "test", "192.168.1.112:9092", "mesh");
    mesh.getHub().register(InformationHandler.class, HitsHandler.class);
    mesh.getHub().run();
    try {
      Thread.sleep(10000);
    }catch (Exception e){

    }
    mesh.call(new String[]{"information.hits", "information", "information.changeme"},
      new TreeMap<String, Object>() {{
        put("wow", "works?");
      }},
      new NucleoResponder() {
        @Override
        public void run(NucleoData data) {
          try {
            System.out.println(new ObjectMapper().writeValueAsString(data));
            System.out.println((data.getExecution().getTotal()) + "ms");
            mesh.call(new String[]{"information.hits", "information"},
              new TreeMap<String, Object>() {{
                put("wow", "works?");
              }},
              new NucleoResponder() {
                @Override
                public void run(NucleoData data) {
                  try {
                    System.out.println(new ObjectMapper().writeValueAsString(data));
                    System.out.println((data.getExecution().getTotal()) + "ms");
                  } catch (Exception e) {
                    e.printStackTrace();
                  }
                }
              });
          } catch (Exception e) {
            e.printStackTrace();
          }
        }
      });
  }
}
