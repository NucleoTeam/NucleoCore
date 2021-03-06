package com.synload.nucleo.chain.link;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.synload.nucleo.chain.path.PathBuilder;
import com.synload.nucleo.chain.path.PathGenerationException;
import com.synload.nucleo.chain.path.Run;

import java.io.Serializable;
import java.lang.reflect.Method;
import java.util.*;
import java.util.stream.Collectors;

public class NucleoLinkMeta implements Serializable {
    public static class RunRequirement extends NucleoLinkMeta {
        private Run Fulfillment = null;
        private boolean immediateFollows;


        public RunRequirement() {
        }
        public RunRequirement(NucleoRequirement nucleoRequirement) throws PathGenerationException {

            super.setAcceptPreviousLinks(nucleoRequirement.acceptPreviousLinks());
            super.setLinkOnly(nucleoRequirement.linkOnly());
            this.setImmediateFollows(nucleoRequirement.immediateFollows());

            if(nucleoRequirement.value() != null && !nucleoRequirement.value().equals("")) {
                this.setChain(nucleoRequirement.value());
                if(isLinkOnly()) {
                    this.setFulfillment(PathBuilder.generateExactRun(nucleoRequirement.value()).getRoot());
                }else{
                    this.setFulfillment(PathBuilder.generateSerialRun(nucleoRequirement.value()).getRoot());
                }
            }else if(nucleoRequirement.chains().length>0) {
                this.setChain(String.join("->", nucleoRequirement.chains()));
                if(isLinkOnly()) {
                    PathBuilder pathBuilder = new PathBuilder();
                    pathBuilder.add(Arrays.stream(nucleoRequirement.chains()).map(n->PathBuilder.generateExactRun(n)).collect(Collectors.toList()));
                    this.setFulfillment(pathBuilder.getStart());
                }else{
                    this.setFulfillment(PathBuilder.generateSerialRun(nucleoRequirement.chains()).getRoot());
                }
            }
            if(!this.isAcceptPreviousLinks()){
                this.getFulfillment().traverseAndModify(i->i.setAlways(true));
            }
        }
        public Run getFulfillment() {
            return Fulfillment;
        }
        public void setFulfillment(Run fulfillment) {
            Fulfillment = fulfillment;
        }
        public boolean isImmediateFollows() {
            return immediateFollows;
        }

        public void setImmediateFollows(boolean immediateFollows) {
            this.immediateFollows = immediateFollows;
        }
    }
    private String chain;
    private boolean linkOnly;
    private boolean acceptPreviousLinks;
    private boolean always;
    private boolean binder;

    @JsonIgnore
    private transient Object object;
    @JsonIgnore
    private transient Method method;

    private List<RunRequirement> requirements = new LinkedList<>();

    public String getChain() {
        return chain;
    }

    public void setChain(String chain) {
        this.chain = chain;
    }

    public boolean isLinkOnly() {
        return linkOnly;
    }

    public void setLinkOnly(boolean linkOnly) {
        this.linkOnly = linkOnly;
    }

    public boolean isAcceptPreviousLinks() {
        return acceptPreviousLinks;
    }

    public void setAcceptPreviousLinks(boolean acceptPreviousLinks) {
        this.acceptPreviousLinks = acceptPreviousLinks;
    }

    public Object getObject() {
        return object;
    }

    public void setObject(Object object) {
        this.object = object;
    }

    public Method getMethod() {
        return method;
    }

    public void setMethod(Method method) {
        this.method = method;
    }

    public List<RunRequirement> getRequirements() {
        return requirements;
    }

    public void setRequirements(List<RunRequirement> requirements) {
        this.requirements = requirements;
    }

    public boolean isAlways() {
        return always;
    }

    public boolean isBinder() {
        return binder;
    }

    public void setBinder(boolean binder) {
        this.binder = binder;
    }

    public void setAlways(boolean always) {
        this.always = always;
    }

    public void fromAnnotation(NucleoLink nucleoLink){
        this.setAlways(nucleoLink.always());
        this.setBinder(nucleoLink.binder());
        this.setLinkOnly(nucleoLink.linkOnly());
    }
}
