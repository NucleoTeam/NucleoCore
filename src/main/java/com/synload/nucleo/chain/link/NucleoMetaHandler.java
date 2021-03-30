package com.synload.nucleo.chain.link;

import com.synload.nucleo.NucleoMesh;
import com.synload.nucleo.hub.HubEventType;

import java.util.HashMap;
import java.util.Map;

public class NucleoMetaHandler {
    private Map<String, NucleoLinkMeta> nucleoLinkMetaMap = new HashMap<>();
    private NucleoMesh mesh = null;

    public NucleoMetaHandler(NucleoMesh mesh) {
        this.mesh = mesh;
    }

    public void add(NucleoLinkMeta nucleoLinkMeta){
        if(mesh!=null) {
            if (!nucleoLinkMetaMap.containsKey(nucleoLinkMeta.getChain())) {
                mesh.getEventHandler().callHubEvent(HubEventType.CHAIN_META_ADDED, mesh, nucleoLinkMeta);
            } else {
                mesh.getEventHandler().callHubEvent(HubEventType.CHAIN_META_UPDATED, mesh, nucleoLinkMeta);
            }
        }
        nucleoLinkMetaMap.put(nucleoLinkMeta.getChain(), nucleoLinkMeta);
    }

    public void remove(NucleoLinkMeta nucleoLinkMeta){

    }

    public Map<String, NucleoLinkMeta> getNucleoLinkMetaMap() {
        return nucleoLinkMetaMap;
    }
}
