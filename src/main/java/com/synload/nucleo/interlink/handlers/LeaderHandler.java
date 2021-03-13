package com.synload.nucleo.interlink.handlers;

import com.synload.nucleo.NucleoMesh;
import com.synload.nucleo.event.NucleoClass;
import com.synload.nucleo.interlink.InterlinkEvent;
import com.synload.nucleo.interlink.InterlinkEventType;
import com.synload.nucleo.zookeeper.ServiceInformation;

@NucleoClass
public class LeaderHandler {
    int leaderThreads = 25;
    @InterlinkEvent(InterlinkEventType.GAIN_LEADER)
    public void gainLeader(NucleoMesh mesh, ServiceInformation serviceInformation, String topicName){
        mesh.getInterlinkManager().subscribeLeader(topicName);
    }
    @InterlinkEvent(InterlinkEventType.CEDE_LEADER)
    public void cedeLeader(NucleoMesh mesh, String topicName) {
        mesh.getInterlinkManager().unsubscribeLeader(topicName);
    }
}
