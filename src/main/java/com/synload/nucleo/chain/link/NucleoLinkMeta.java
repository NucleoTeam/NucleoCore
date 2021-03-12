package com.synload.nucleo.chain.link;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.synload.nucleo.chain.path.PathBuilder;
import com.synload.nucleo.chain.path.Run;

import java.io.Serializable;
import java.lang.reflect.Method;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

public class NucleoLinkMeta implements Serializable {
    public static class RunRequirement extends NucleoLinkMeta {
        private Run Fulfillment = null;
        private boolean immediateFollows;


        public RunRequirement() {
        }
        public RunRequirement(NucleoRequirement nucleoRequirement){

            super.setAcceptPreviousLinks(nucleoRequirement.acceptPreviousLinks());
            super.setLinkOnly(nucleoRequirement.linkOnly());
            this.setImmediateFollows(nucleoRequirement.immediateFollows());

            if(nucleoRequirement.value() != null && !nucleoRequirement.value().equals(""))
                this.setFulfillment(PathBuilder.generateSerialRun(nucleoRequirement.value()).getRoot());
            else if(nucleoRequirement.chains().length>0)
                this.setFulfillment(PathBuilder.generateSerialRun(nucleoRequirement.chains()).getRoot());
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

    public void fromAnnotation(NucleoLink nucleoLink){
        this.setAcceptPreviousLinks(nucleoLink.acceptPreviousLinks());
        this.setLinkOnly(nucleoLink.linkOnly());
    }
}
