package com.synload.nucleo.interlink;

import com.synload.nucleo.NucleoMesh;
import com.synload.nucleo.data.NucleoData;
import com.synload.nucleo.zookeeper.ServiceInformation;

public interface InterlinkClient extends Runnable{
    // InterlinkClient(ServiceInformation serviceInformation, InterlinkHandler interlinkHandler);
    void add(String topic, NucleoData data);
    boolean isConnected();
    ServiceInformation getServiceInformation();
}
