package com.synload.nucleo.chain;

import com.google.common.collect.Sets;
import com.synload.nucleo.chain.link.NucleoLink;
import com.synload.nucleo.chain.link.NucleoLinkMeta;
import com.synload.nucleo.chain.link.NucleoRequirement;
import com.synload.nucleo.chain.path.PathGenerationException;
import com.synload.nucleo.event.ClassScanner;
import com.synload.nucleo.event.EventType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Method;
import java.util.*;
import java.util.stream.Collectors;

public class ChainHandler {
    protected static final Logger logger = LoggerFactory.getLogger(ChainHandler.class);
    private Map<String, NucleoLinkMeta> links = new TreeMap<>();

    public NucleoLinkMeta getChainToMethod(String key) {
        if (links.containsKey(key)) {
            return links.get(key);
        }
        return null;
    }
    private void registerLinkToChain(Object object, Method method, NucleoLink nucleoLink,  List<NucleoLinkMeta.RunRequirement> requirements, String chain){
        List<String> parameterTypes = Arrays.stream(method.getParameterTypes()).map(p->p.getSimpleName()).collect(Collectors.toList());
        logger.info("Link[ "+chain+" ] <= "+object.getClass().getName()+"->"+method.getName()+"( "+String.join(", ", parameterTypes)+" )");
        if(requirements.size()>0)
            logger.info("\t"+chain+" requires "+String.join(", ", requirements.stream().map(x->x.getChain()).collect(Collectors.toList())));
        NucleoLinkMeta nucleoLinkMeta = new NucleoLinkMeta();
        nucleoLinkMeta.setRequirements(requirements);
        nucleoLinkMeta.fromAnnotation(nucleoLink);
        nucleoLinkMeta.setObject(object);
        nucleoLinkMeta.setChain(chain);
        nucleoLinkMeta.setMethod(method);
        links.put(chain, nucleoLinkMeta);
    }
    public void registerLink(Object object, Method method) throws PathGenerationException {
        NucleoLink nEvent = method.getAnnotation(NucleoLink.class);
        String chain = nEvent.value();
        NucleoRequirement[] requirements = method.getAnnotationsByType(NucleoRequirement.class);
        List<NucleoLinkMeta.RunRequirement> runRequirements = new LinkedList<>();
        if(requirements.length>0){
            for (int i = 0; i < requirements.length; i++) {
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

    public Map<String, NucleoLinkMeta> getLinks() {
        return links;
    }

    public void setLinks(Map<String, NucleoLinkMeta> links) {
        this.links = links;
    }
}
