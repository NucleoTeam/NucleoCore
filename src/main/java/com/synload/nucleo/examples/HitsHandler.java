package com.synload.nucleo.examples;

import com.synload.nucleo.event.NucleoClass;
import com.synload.nucleo.data.NucleoData;
import com.synload.nucleo.event.NucleoLink;

@NucleoClass
public class HitsHandler {
  @NucleoLink("information.hits")
  public NucleoData hitCount(NucleoData data){
    data.getObjects().createOrUpdate("test","run");
    return data;
  }

  @NucleoLink("information.test")
  public NucleoData anotherTest(NucleoData data){
    data.getObjects().createOrUpdate("papa","kek");
    return data;
  }

  /*
     Only execute information.changeme only after information.hits is run
   */
  @NucleoLink("information.hits > information.changeme")
  public NucleoData changeMe(NucleoData data){
    data.getObjects().createOrUpdate("wow", "kekekekeke");
    return data;
  }

}
