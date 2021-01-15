package com.synload.nucleo.information;

import com.synload.nucleo.event.NucleoClass;
import com.synload.nucleo.data.NucleoData;
import com.synload.nucleo.event.NucleoEvent;

@NucleoClass
public class HitsHandler {
  @NucleoEvent("information.hits")
  public NucleoData hitCount(NucleoData data){
    data.getObjects().createOrUpdate("test","run");
    return data;
  }

  @NucleoEvent("information.test")
  public NucleoData anotherTest(NucleoData data){
    data.getObjects().createOrUpdate("papa","kek");
    return data;
  }

  /*
     Only execute information.changeme only after information.hits is run
   */
  @NucleoEvent("information.hits > information.changeme")
  public NucleoData changeMe(NucleoData data){
    data.getObjects().createOrUpdate("wow", "kekekekeke");
    return data;
  }

}
