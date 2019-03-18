package com.synload.nucleo.information;

import com.synload.nucleo.event.NucleoData;
import com.synload.nucleo.event.NucleoEvent;

public class HitsHandler {
  @NucleoEvent("info.hits")
  public NucleoData hitCount(NucleoData data){
    return data;
  }

}
