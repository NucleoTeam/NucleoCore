package com.synload.nucleo.information;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.synload.nucleo.event.NucleoClass;
import com.synload.nucleo.event.NucleoData;
import com.synload.nucleo.event.NucleoEvent;
import com.synload.nucleo.event.NucleoResponder;

import java.io.Serializable;

@NucleoClass
public class InformationHandler {
  @NucleoEvent(chains={"information"})
  public NucleoData information(NucleoData data){
    if(data.getObjects().containsKey("stop")){
      data.getChainBreak().setBreakChain(true);
      return data;
    }
    data.getObjects().put("taco", "bell");
    return data;
  }
  @NucleoEvent(chains={"popcorn"})
  public void popcorn(NucleoData data, NucleoResponder r){
    data.getObjects().put("POPPY", "CORN");
    r.run(data);
  }
  @NucleoEvent(chains={"information.popcorn"})
  public void infoPopcorn(NucleoData data, NucleoResponder r){
    data.getObjects().put("information.popcorn", "set");
    r.run(data);
  }
  @NucleoEvent("information.popcorn > popcorn.poppyx")
  public NucleoData poppyx(NucleoData data){
    data.getObjects().put("POP", "LOCK");
    return data;
  }
  @NucleoEvent("information.popcorn,information.test > popcorn.poppy")
  public NucleoData poppy(NucleoData data){
    data.getObjects().put("TOP", "TOP");
    return data;
  }
}
