package com.synload.nucleo.interlink;

import com.synload.nucleo.data.NucleoData;

public interface InterlinkHandler {
    void handleMessage(String topic, NucleoData data);
}
