package com.synload.nucleo.hub;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.synload.nucleo.event.NucleoChain;
import com.synload.nucleo.data.NucleoData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class TrafficHandler {
    protected static final Logger logger = LoggerFactory.getLogger(TrafficHandler.class);
    private Map<String, List<NucleoData>> parallelParts = Maps.newHashMap();

    public List<NucleoData> renderParallel(NucleoData data, NucleoChain chain) {
        List<NucleoData> nextDatas = new ArrayList<>();
        for (int i = 0; i < chain.getParallelChains().size(); i++) {
            data.getChainList().get(data.getOnChain()).setParallel(i);
            NucleoData newData = new NucleoData(data);
            nextDatas.add(newData);
        }
        return nextDatas;
    }

    public List<NucleoData> renderSingle(NucleoData data, NucleoChain chain) {
        List<NucleoData> nextDatas = new ArrayList<>();
        nextDatas.add(new NucleoData(data));
        return nextDatas;
    }


    public List<NucleoData> render(NucleoData data, NucleoChain chain) {
        if (chain == null)
            return renderSingle(data, chain);
        if (chain.getChainString() != null)
            return renderSingle(data, chain);
        if (chain.getParallelChains().size() > 0)
            return renderParallel(data, chain);

        return Lists.newLinkedList();
    }

    public List<NucleoData> getNext(NucleoData data) {
        if (data.getChainList().size() == 0 || data.getChainList().size() <= data.getOnChain())
            return null;

        if (data.getOnChain() == -1) {
            data.setOnChain(0);
            return render(data, data.currentChain());
        }
        NucleoChain currentChain = data.currentChain();
        if (currentChain != null && currentChain.getChainString().length > currentChain.getPart() + 1) {
            currentChain.setPart(currentChain.getPart() + 1);
            return render(data, currentChain);
        }
        data.currentChain().setComplete(true);
        data.currentParentChain().setComplete(true);
        data.setOnChain(data.getOnChain() + 1);
        if (data.getTrack() != 0)
            try {
                logger.debug("render: " + new ObjectMapper().writeValueAsString(data));
            } catch (Exception e) {
                e.printStackTrace();
            }
        return render(data, data.currentParentChain());
    }

    public synchronized void addPart(NucleoData data) {
        String root = data.getRoot().toString();
        if (!this.parallelParts.containsKey(root))
            this.parallelParts.put(root, Lists.newLinkedList());

        if (parallelParts.get(data.getRoot().toString()).stream().filter(
            part -> part.previousParentChain().getParallel() == data.previousParentChain().getParallel()
        ).count() == 0) {
            this.parallelParts.get(root).add(data);
        }
    }

    public boolean allPartsReceived(NucleoData data, NucleoChain asyncChain) {
        return this.parallelParts.get(data.getRoot().toString()).size() == asyncChain.getParallelChains().size();
    }

    public void processParallel(NucleoData data, TrafficExecutor responder) {

        NucleoChain previousChain = data.previousParentChain();
        String root = data.getRoot().toString();

        if (data.getTrack() == 1) logger.debug(root + ": parallel received");

        if (previousChain != null && previousChain.getParallelChains().size() > 0 && !previousChain.isRecombined()) {
            if (previousChain.isComplete()) {
                addPart(data);
                if (data.getTrack() == 1) logger.debug(root + ": parallel part added");
                if (!allPartsReceived(data, previousChain)) {
                    // all parts not yet received
                    if (data.getTrack() == 1) logger.debug(root + ": waiting for all parts");
                } else {
                    // all parts received
                    if (data.getTrack() == 1) logger.debug(root + ": all parts received");
                    List<NucleoData> paraParts = parallelParts.remove(root);
                    NucleoData finalPart = null;
                    for (NucleoData part : paraParts) {
                        NucleoChain previousChainPart = part.previousParentChain();
                        int parallel = previousChainPart.getParallel();
                        previousChainPart = previousChainPart.getParallelChains().get(parallel);
                        if (finalPart != null) {
                            boolean saveChanges = false;
                            for (int i = 0; i < part.getObjects().getChanges().size(); i++) {
                                if (
                                    finalPart.getObjects().getChanges().size() == 0 ||
                                        (!saveChanges
                                            && !finalPart.getObjects().getChanges().get(i).equals(part.getObjects().getChanges().get(i)))
                                ) {
                                    saveChanges = true;
                                    if (saveChanges) {
                                        finalPart.getObjects().getChanges().add(part.getObjects().getChanges().get(i));
                                    }
                                }
                            }
                            int stepStart = previousChainPart.stepStart;
                            if (stepStart != -1) {
                                previousChainPart.setStepStart(finalPart.getSteps().size());
                                for (int i = stepStart; i < part.getSteps().size(); i++) {
                                    finalPart.getSteps().add(part.getSteps().get(i));
                                }
                            }
                            finalPart.previousParentChain().getParallelChains().set(parallel, previousChainPart);
                            previousChainPart.setRecombined(true);
                        } else {
                            previousChainPart.setRecombined(true);
                            finalPart = part;
                        }
                    }
                    if (data.getTrack() == 1) logger.debug(root + ": executing");
                    finalPart.previousParentChain().setRecombined(true);
                    responder.setData(finalPart);
                    responder.handle();
                }
            }
        } else {
            responder.handle();
        }
    }
}
