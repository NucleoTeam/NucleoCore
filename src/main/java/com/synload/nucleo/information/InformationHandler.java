package com.synload.nucleo.information;

import com.synload.nucleo.event.NucleoClass;
import com.synload.nucleo.data.NucleoData;
import com.synload.nucleo.event.NucleoEvent;
import com.synload.nucleo.event.NucleoResponder;

@NucleoClass
public class InformationHandler {
  @NucleoEvent(chains={"information"})
  public NucleoData information(NucleoData data){
    if(data.latestObjects().exists("stop")){
      data.getChainBreak().setBreakChain(true);
      return data;
    }
    data.latestObjects().set("taco", "bell");
    return data;
  }
  @NucleoEvent(chains={"popcorn"})
  public void popcorn(NucleoData data, NucleoResponder r){
    data.latestObjects().set("POPPY", "CORN");
    r.run(data);
  }
  @NucleoEvent(chains={"information.popcorn"})
  public void infoPopcorn(NucleoData data, NucleoResponder r){
    data.latestObjects().set("information-popcorn", "set");
    r.run(data);
  }
  @NucleoEvent("information.popcorn > popcorn.poppyx")
  public NucleoData poppyx(NucleoData data){
    data.latestObjects().set("POP", "LOCK");
    return data;
  }
  @NucleoEvent("information.popcorn,information.test > popcorn.poppy")
  public NucleoData poppy(NucleoData data){
    data.latestObjects().set("TOP", "TOP");
    return data;
  }
}
