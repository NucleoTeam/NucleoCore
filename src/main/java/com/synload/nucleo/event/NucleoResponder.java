package com.synload.nucleo.event;

import com.synload.nucleo.data.NucleoData;

public interface NucleoResponder {
  void run(NucleoData data);
}
