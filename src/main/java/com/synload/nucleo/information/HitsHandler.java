package com.synload.nucleo.information;

import com.synload.nucleo.event.NucleoData;
import com.synload.nucleo.event.NucleoEvent;

public class HitsHandler {
  @NucleoEvent("information.hits")
  public NucleoData hitCount(NucleoData data){
    data.getObjects().put("test","run");
    return data;
  }
  @NucleoEvent("information.hits > information.changeme")
  public NucleoData changeMe(NucleoData data){
    data.getObjects().put("wow", "Changed the value from another application");
    return data;
  }
}
