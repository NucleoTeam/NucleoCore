package com.synload.nucleo.information;

import com.synload.nucleo.event.NucleoData;
import com.synload.nucleo.event.NucleoEvent;

public class HitsHandler {
  @NucleoEvent("information.hits")
  public NucleoData hitCount(NucleoData data){
    data.getObjects().put("test","run");
    return data;
  }
  /*
     Only execute information.changeme only after information.hits is run
   */
  @NucleoEvent("information.hits > test > information.changeme")
  public NucleoData changeMe(NucleoData data){
    data.getObjects().put("wow", "kekekekeke");
    return data;
  }
}
