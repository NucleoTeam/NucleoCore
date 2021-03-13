package com.synload.nucleo.interlink.handlers;

import com.synload.nucleo.chain.path.SingularRun;
import com.synload.nucleo.data.NucleoData;
import com.synload.nucleo.event.NucleoClass;
import com.synload.nucleo.interlink.InterlinkEvent;
import com.synload.nucleo.interlink.InterlinkEventType;
import com.synload.nucleo.zookeeper.ServiceInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@NucleoClass
public class TopicHandler {
    protected static final Logger logger = LoggerFactory.getLogger(TopicHandler.class);

    @InterlinkEvent(InterlinkEventType.RECEIVE_TOPIC)
    public void receiveTopic(NucleoData data){
        logger.debug("Received message on topic "+((SingularRun)data.getChainExecution().getCurrent()).getChain());
    }
    @InterlinkEvent(InterlinkEventType.SEND_TOPIC)
    public void sendTopic(NucleoData data){
        logger.debug("Sending message to topic "+((SingularRun)data.getChainExecution().getCurrent()).getChain());
    }
}
