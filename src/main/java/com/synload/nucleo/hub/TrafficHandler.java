package com.synload.nucleo.hub;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.synload.nucleo.event.NucleoChain;
import com.synload.nucleo.data.NucleoData;
import com.synload.nucleo.event.NucleoStep;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public class TrafficHandler {
    protected static final Logger logger = LoggerFactory.getLogger(TrafficHandler.class);
    private Map<String, List<NucleoData>> parallelParts = Maps.newHashMap();

    public List<NucleoData> renderParallel(NucleoData data, NucleoChain chain) {
        List<NucleoData> nextDatas = new ArrayList<>();
        for (int i = 0; i < chain.getParallelChains().size(); i++) {
            data.getChainList().get(data.getOnChain()).setParallel(i);
            NucleoData newData = new NucleoData(data);
            newData.getObjects().setLedgerMode(true);
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
            return renderSingle(data, null);
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
            data.getObjects().setStep(0);
            return render(data, data.currentChain());
        }
        NucleoChain currentChain = data.currentChain();
        if (currentChain != null && currentChain.getChainString().length > currentChain.getPart() + 1) {
            currentChain.setPart(currentChain.getPart() + 1);
            data.getObjects().setStep(data.getOnChain() + 1);
            return render(data, currentChain);
        }
        data.currentChain().setComplete(true);
        data.currentParentChain().setComplete(true);
        data.setOnChain(data.getOnChain() + 1);
        data.getObjects().setStep(data.getOnChain() + 1);
        if (data.getTrack() != 0)
            try {
                logger.debug("render: " + new ObjectMapper().writeValueAsString(data));
            } catch (Exception e) {
                e.printStackTrace();
            }
        return render(data, data.currentParentChain());
    }

    public void addPart(NucleoData data) {
        String root = data.getRoot().toString();
        if (!this.parallelParts.containsKey(root))
            this.parallelParts.put(root, Lists.newLinkedList());

        if (parallelParts
            .get(data.getRoot().toString())
            .stream()
            .filter(part -> part.previousParentChain().getParallel() == data.previousParentChain().getParallel())
            .count() == 0
        ) {
            this.parallelParts.get(root).add(data);
        }
    }

    public boolean allPartsReceived(NucleoData data, NucleoChain asyncChain) {
        UUID root = data.getRoot();
        if(this.parallelParts.containsKey(root.toString())){
            List<NucleoData> nucleoData = this.parallelParts.get(root.toString());
            return nucleoData.size() == asyncChain.getParallelChains().size();
        }
        return false;
    }

    public synchronized void processParallel(NucleoData data, TrafficExecutor responder) {

        NucleoChain previousChain = data.previousParentChain();
        String root = data.getRoot().toString();

        if (data.getTrack() == 1){
            if(data.getSteps().size()>0) {
                NucleoStep step = data.getSteps().get(data.getSteps().size() - 1);
                logger.debug(root + ": parallel received from " + step.getHost() + " -> " + step.getStep());
            }else{
                logger.debug(root + ": parallel received but did not come from a step!");
            }

        }

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
                    if (paraParts != null) {
                        NucleoData finalPart = null;
                        for (NucleoData part : paraParts) {
                            NucleoChain previousChainPart = part.previousParentChain();
                            int parallel = previousChainPart.getParallel();
                            previousChainPart = previousChainPart.getParallelChains().get(parallel);
                            if (finalPart != null) {
                                logger.info("Final Part Merged "+root+" <= "+part.getSteps().get(part.getSteps().size()-1).getStep());
                                finalPart.getObjects().getChanges().addAll(part.getObjects().getChanges());
                                finalPart.getObjects().setLedgerMode(false);
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
                                logger.info("Merged "+root+" <= "+part.getSteps().get(part.getSteps().size()-1).getStep());
                                previousChainPart.setRecombined(true);
                                finalPart = part;
                            }
                        }
                        if (data.getTrack() == 1) logger.debug(root + ": executing");
                        finalPart.getObjects().buildFinalizedState();
                        finalPart.previousParentChain().setRecombined(true);
                        responder.setData(finalPart);
                        responder.handle();
                    }
                }
            }
        } else {
            responder.handle();
        }
    }
}
