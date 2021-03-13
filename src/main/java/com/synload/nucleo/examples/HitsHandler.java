package com.synload.nucleo.examples;

import com.synload.nucleo.event.NucleoClass;
import com.synload.nucleo.data.NucleoData;
import com.synload.nucleo.chain.link.NucleoLink;
import com.synload.nucleo.chain.link.NucleoRequirement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@NucleoClass
public class HitsHandler {
  protected static final Logger logger = LoggerFactory.getLogger(HitsHandler.class);

  @NucleoLink("information.hits")
  public NucleoData hitCount(NucleoData data){
    data.getObjects().createOrUpdate("test","run");
    logger.info(data.getRoot().toString()+" processing hitCount");
    return data;
  }
  // information -> information.hits -> information -> information.test
  @NucleoLink("information.test")
  public NucleoData anotherTest(NucleoData data){
    data.getObjects().createOrUpdate("papa","kek");
    logger.info(data.getRoot().toString()+" processing anotherTest");
    return data;
  }

  /*
     Only execute information.changeme only after information.hits is run
   */
  @NucleoRequirement(value = "information.hits", acceptPreviousLinks = true) // can ignore information if run previously
  @NucleoLink(value = "information.changeme", acceptPreviousLinks = true) // can ignore information if run previously
  public NucleoData changeMe(NucleoData data){
    data.getObjects().createOrUpdate("wow", "kekekekeke");
    logger.info(data.getRoot().toString()+" processing changeMe");
    return data;
  }

}
