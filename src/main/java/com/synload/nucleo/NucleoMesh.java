package com.synload.nucleo;

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

  public Hub getHub() {
    return hub;
}

  public static void main(String[] args){
    //createTopic();
    Logger.getRootLogger().setLevel(Level.DEBUG);
    NucleoMesh mesh = new NucleoMesh( "root","192.168.1.122:9092", "mesh");
    mesh.getHub().register(new InformationHandler(), new HitsHandler());
    mesh.getHub().run();
    while(mesh.getHub().isReady()) {
      try {
        Thread.sleep(1L);
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
    /*TreeMap<String, Object> data = new TreeMap<String, Object>();
    data.put("wow", "works?");
    mesh.call("info.hits", data, new NucleoResponder(){
      @Override
      public void run(NucleoData data) {
        System.out.println(data.getObjects().get("wow"));
      }
    });*/
  }
}
