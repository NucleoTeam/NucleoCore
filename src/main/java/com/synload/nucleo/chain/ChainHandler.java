package com.synload.nucleo.chain;

import com.google.common.collect.Sets;
import com.synload.nucleo.chain.link.NucleoLink;
import com.synload.nucleo.chain.link.NucleoLinkMeta;
import com.synload.nucleo.chain.link.NucleoRequirement;
import com.synload.nucleo.event.ClassScanner;
import com.synload.nucleo.event.EventType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Method;
import java.util.*;

public class ChainHandler {
    protected static final Logger logger = LoggerFactory.getLogger(ChainHandler.class);
    private Map<String, NucleoLinkMeta> links = new TreeMap<>();
    public Map<String, NucleoLinkMeta> getChainToMethod() {
        return links;
    }

    public NucleoLinkMeta getChainToMethod(String key) {
        if (links.containsKey(key)) {
            return links.get(key);
        }
        return null;
    }
    private void registerLinkToChain(Object object, Method method, NucleoLink nucleoLink,  List<NucleoLinkMeta.RunRequirement> requirements, String chain){
        NucleoLinkMeta nucleoLinkMeta = new NucleoLinkMeta();
        nucleoLinkMeta.setRequirements(requirements);
        nucleoLinkMeta.fromAnnotation(nucleoLink);
        nucleoLinkMeta.setObject(object);
        nucleoLinkMeta.setMethod(method);
        links.put(chain, nucleoLinkMeta);
    }
    public void registerLink(Object object, Method method) {
        NucleoLink nEvent = method.getAnnotation(NucleoLink.class);
        String chain = nEvent.value();
        NucleoRequirement[] requirements = method.getAnnotationsByType(NucleoRequirement.class);
        List<NucleoLinkMeta.RunRequirement> runRequirements = new LinkedList<>();
        if(requirements.length>0){
            for (int i = 0; i < requirements.length; i++) {
                logger.info("requirement found.");
                runRequirements.add(new NucleoLinkMeta.RunRequirement(requirements[i]));
            }
        }
        if (chain.equals("") && nEvent.chains().length > 0) {
            String[] chains = nEvent.chains();
            if (chains.length > 0) {
                for (String chainString : chains) {
                    registerLinkToChain(object, method, nEvent, runRequirements, chainString);
                }
            }
        } else {
            registerLinkToChain(object, method, nEvent, runRequirements, chain);
        }
    }
}
