package com.synload.nucleo.interlink.handlers;

import com.synload.nucleo.event.NucleoClass;
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
    public void newService(ServiceInformation serviceInformation){
        logger.debug(serviceInformation.getService()+" ( "+serviceInformation.getUniqueName()+" ) joined cluster.");
    }

    @InterlinkEvent(InterlinkEventType.LEAVE_SERVICE)
    public void leaveService(ServiceInformation serviceInformation){
        logger.debug(serviceInformation.getService()+" ( "+serviceInformation.getUniqueName()+" ) left cluster.");
    }
}
