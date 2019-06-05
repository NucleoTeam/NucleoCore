package com.synload.nucleo.information;

import com.synload.nucleo.event.NucleoData;
import com.synload.nucleo.event.NucleoEvent;

public class InformationHandler {
  @NucleoEvent(chains={"information"})
  public NucleoData hitCount(NucleoData data){
    if(data.getObjects().containsKey("stop")){
      data.getChainBreak().setBreakChain(true);
      return data;
    }
    data.getObjects().put("taco", "bell");
    return data;
  }
  @NucleoEvent(chains={"popcorn"})
  public NucleoData popcorn(NucleoData data){
    data.getObjects().put("POPPY", "CORN");
    return data;
  }
}
