package com.synload.nucleo.interlink.handlers;

import com.synload.nucleo.NucleoMesh;
import com.synload.nucleo.chain.link.NucleoLinkMeta;
import com.synload.nucleo.event.NucleoClass;
import com.synload.nucleo.hub.HubEvent;
import com.synload.nucleo.hub.HubEventType;
import com.synload.nucleo.interlink.InterlinkEvent;
import com.synload.nucleo.interlink.InterlinkEventType;
import com.synload.nucleo.zookeeper.ServiceInformation;
import com.synload.nucleo.zookeeper.ZooKeeperManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@NucleoClass
public class ServiceHandler {
    protected static final Logger logger = LoggerFactory.getLogger(ServiceHandler.class);

    @InterlinkEvent(InterlinkEventType.NEW_SERVICE)
    public void newService(NucleoMesh mesh, ServiceInformation serviceInformation){
        serviceInformation.getEvents().forEach(e->mesh.getNucleoMetaHandler().add(e));
        logger.info(serviceInformation.getService()+" ( "+serviceInformation.getUniqueName()+" ) joined cluster.");
    }

    @InterlinkEvent(InterlinkEventType.LEAVE_SERVICE)
    public void leaveService(NucleoMesh mesh, ServiceInformation serviceInformation){
        logger.info(serviceInformation.getService()+" ( "+serviceInformation.getUniqueName()+" ) left cluster.");
    }

    @HubEvent(HubEventType.CHAIN_META_ADDED)
    public void addedEventMeta(NucleoMesh mesh, NucleoLinkMeta linkMeta){
        logger.info("Meta information for " + linkMeta.getChain() + ". added to the local meta repository.");
    }

    @HubEvent(HubEventType.CHAIN_META_UPDATED)
    public void updatedEventMeta(NucleoMesh mesh, NucleoLinkMeta linkMeta){
        logger.info("Meta information for " + linkMeta.getChain() + ". updated in the local meta repository.");
    }
}
