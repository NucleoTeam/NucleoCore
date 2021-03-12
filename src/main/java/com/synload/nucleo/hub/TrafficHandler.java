package com.synload.nucleo.hub;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.synload.nucleo.chain.ChainExecution;
import com.synload.nucleo.chain.path.Run;
import com.synload.nucleo.chain.path.SingularRun;
import com.synload.nucleo.data.NucleoData;
import com.synload.nucleo.data.NucleoStep;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;

public class TrafficHandler {
    protected static final Logger logger = LoggerFactory.getLogger(TrafficHandler.class);
    private Map<String, List<NucleoData>> parallelParts = Maps.newHashMap();

    public List<NucleoData> getNext(NucleoData data) {
        return data.getChainExecution().next().stream().map(ce->{
            NucleoData nd = new NucleoData(data, ce);
            nd.getObjects().setLedgerMode(ce.getCurrent().isParallel());
            return nd;
        }).collect(Collectors.toList());
    }

    public void addPart(NucleoData data) {
        String root = data.getRoot().toString();
        if (!this.parallelParts.containsKey(root))
            this.parallelParts.put(root, Lists.newLinkedList());

        if (parallelParts
            .get(data.getRoot().toString())
            .stream()
            .filter(part -> part.getChainExecution().getHistory().equals(data.getChainExecution().getHistory()))
            .count() == 0
        ) {
            this.parallelParts.get(root).add(data);
        }
    }

    public synchronized void process(NucleoData data, TrafficExecutor responder) {
        String dataUUID = data.getRoot().toString();
        Set<Run> previousChain = data.getChainExecution().getCurrent().getParents();
        // wait for all parts.
        long previousParallelCount = previousChain.stream().filter(f->f.isParallel()).count();
        if(previousParallelCount>0 && !data.getChainExecution().getCurrent().isParallel()){
            addPart(data);
            if (data.getTrack() == 1) logger.debug(dataUUID + ": parallel part added");
            if(this.parallelParts.containsKey(dataUUID)){
                if(this.parallelParts.get(dataUUID).size() == previousParallelCount){
                    if (data.getTrack() == 1) logger.debug(dataUUID + ": all parts received");
                    List<NucleoData> parts = this.parallelParts.remove(dataUUID);
                    NucleoData finalPart = null;
                    for (NucleoData part : parts) {
                        if (finalPart != null) {
                            if (data.getTrack() == 1) logger.info("Final Part Merged "+dataUUID);
                            finalPart.getObjects().getChanges().addAll(part.getObjects().getChanges());
                            List<String> history = finalPart.getChainExecution().getHistory();
                            List<String> historyPart = part.getChainExecution().getHistory();
                            int x=0;
                            while(history.get(x).equals(historyPart.get(x))){
                                x++;
                            }
                            history.addAll(historyPart.subList(x,history.size()));
                        } else {
                            if (data.getTrack() == 1) logger.info("Merged "+dataUUID);
                            finalPart = part;
                        }
                    }
                    if (data.getTrack() == 1) logger.debug(dataUUID + ": executing");
                    finalPart.getObjects().setLedgerMode(false);
                    finalPart.getObjects().buildFinalizedState();
                    responder.setData(finalPart);
                    responder.handle();
                }
            }else{
                if (data.getTrack() == 1) logger.debug(dataUUID + ": waiting for all parts");
            }
        }else if(previousParallelCount>0 && data.getChainExecution().getCurrent().isParallel()){
            // still in parallel
            responder.handle();
        }else if(previousParallelCount==0 && !data.getChainExecution().getCurrent().isParallel()){
            // still out of parallel
            responder.handle();
        }
    }
}
