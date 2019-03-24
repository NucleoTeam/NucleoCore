package com.synload.nucleo.information;

import com.synload.nucleo.event.NucleoData;
import com.synload.nucleo.event.NucleoEvent;

public class InformationHandler {
  @NucleoEvent("information")
  public NucleoData hitCount(NucleoData data){
    if(data.getObjects().containsKey("stop")){
      data.getChainBreak().setBreakChain(true);
      return null;
    }
    data.getObjects().put("taco", "bell");
    return data;
  }
}
