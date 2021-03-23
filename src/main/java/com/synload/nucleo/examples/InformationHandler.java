package com.synload.nucleo.examples;

import com.synload.nucleo.chain.link.NucleoRequirement;
import com.synload.nucleo.event.NucleoClass;
import com.synload.nucleo.data.NucleoData;
import com.synload.nucleo.chain.link.NucleoLink;
import com.synload.nucleo.event.NucleoResponder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@NucleoClass
public class InformationHandler {
  protected static final Logger logger = LoggerFactory.getLogger(InformationHandler.class);

  @NucleoLink("information")
  public NucleoData information(NucleoData data){
    if(data.getObjects().exists("stop")){
      data.getChainBreak().setBreakChain(true);

      return data;
    }
    data.getObjects().createOrUpdate("taco", "bell");

    return data;
  }

  @NucleoLink("popcorn")
  public void popcorn(NucleoData data, NucleoResponder r){
    data.getObjects().createOrUpdate("POPPY", "CORN");
    r.run(data);
  }

  @NucleoLink(chains={"information.popcorn"})
  public void infoPopcorn(NucleoData data, NucleoResponder r){
    data.getObjects().createOrUpdate("information-popcorn", "set");
    r.run(data);
  }

  @NucleoRequirement("information.popcorn")
  @NucleoLink("popcorn.poppyx")
  public NucleoData poppyx(NucleoData data){
    data.getObjects().createOrUpdate("POP", "LOCK");
    return data;
  }

  @NucleoRequirement("information.popcorn")
  @NucleoRequirement("information.test")
  @NucleoLink("popcorn.poppy")
  public NucleoData poppy(NucleoData data){
    data.getObjects().createOrUpdate("TOP", "TOP");
    return data;
  }
}
