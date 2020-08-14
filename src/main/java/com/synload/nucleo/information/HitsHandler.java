package com.synload.nucleo.information;

import com.synload.nucleo.event.NucleoClass;
import com.synload.nucleo.data.NucleoData;
import com.synload.nucleo.event.NucleoEvent;

@NucleoClass
public class HitsHandler {
  @NucleoEvent("information.hits")
  public NucleoData hitCount(NucleoData data){
    data.latestObjects().set("test","run");
    return data;
  }

  @NucleoEvent("information.test")
  public NucleoData anotherTest(NucleoData data){
    data.latestObjects().set("papa","kek");
    return data;
  }

  /*
     Only execute information.changeme only after information.hits is run
   */
  @NucleoEvent("information.hits > information.changeme")
  public NucleoData changeMe(NucleoData data){
    data.latestObjects().set("wow", "kekekekeke");
    return data;
  }

}
