package com.synload.nucleo.examples;

import com.synload.nucleo.event.NucleoClass;
import com.synload.nucleo.data.NucleoData;
import com.synload.nucleo.chain.link.NucleoLink;
import com.synload.nucleo.chain.link.NucleoRequirement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@NucleoClass
public class UserHandler {
  protected static final Logger logger = LoggerFactory.getLogger(UserHandler.class);

  @NucleoLink("user")
  public void getUser(){

  }

  @NucleoLink(value = "user.login", always = true)
  public NucleoData hitCount(NucleoData data){
    data.getObjects().createOrUpdate("sessionId","run");
    return data;
  }

  // information -> information.hits -> information -> information.test
  @NucleoRequirement(value = "information.hits", acceptPreviousLinks = true)
  @NucleoLink("information.test")
  public NucleoData anotherTest(NucleoData data){
    data.getObjects().createOrUpdate("papa","kek");
    return data;
  }

  /*
     Only execute information.changeme only after information.hits is run
   */
  @NucleoRequirement(value = "information.hits", acceptPreviousLinks = true, linkOnly = true) // can ignore information.hits if run previously
  @NucleoLink(value = "information.changeme", always = true) // can ignore information if run previously
  public NucleoData changeMe(NucleoData data){
    data.getObjects().createOrUpdate("wow", "kekekekeke");
    return data;
  }

}
