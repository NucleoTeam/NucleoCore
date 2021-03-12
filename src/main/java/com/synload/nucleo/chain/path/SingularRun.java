package com.synload.nucleo.chain.path;

public class SingularRun extends Run {
    String chain;

    public SingularRun(String chain) {
        this.chain = chain;
    }

    public String getChain() {
        return chain;
    }

    public void setChain(String chain) {
        this.chain = chain;
    }
}
