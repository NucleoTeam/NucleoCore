package com.synload.nucleo.examples;

import com.synload.nucleo.event.NucleoClass;
import com.synload.nucleo.data.NucleoData;
import com.synload.nucleo.chain.link.NucleoLink;
import com.synload.nucleo.chain.link.NucleoRequirement;

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
  @NucleoRequirement(value = "information.hits", acceptPreviousLinks = true) // can ignore information if run previously
  @NucleoLink(value = "information.changeme", acceptPreviousLinks = true) // can ignore information if run previously
  public NucleoData changeMe(NucleoData data){
    data.getObjects().createOrUpdate("wow", "kekekekeke");
    return data;
  }

}
