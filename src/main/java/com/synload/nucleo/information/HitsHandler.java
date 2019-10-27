package com.synload.nucleo.information;

import com.synload.nucleo.event.NucleoClass;
import com.synload.nucleo.event.NucleoData;
import com.synload.nucleo.event.NucleoEvent;

@NucleoClass
public class HitsHandler {
  @NucleoEvent("information.hits")
  public NucleoData hitCount(NucleoData data){
    data.getObjects().put("test","run");
    try {
      Thread.sleep(100);
    }catch (Exception e){
      e.printStackTrace();
    }
    return data;
  }
  /*
     Only execute information.changeme only after information.hits is run
   */
  @NucleoEvent("information.hits > information.changeme")
  public NucleoData changeMe(NucleoData data){
    data.getObjects().put("wow", "kekekekeke");
    return data;
  }
}
