package com.synload.nucleo.path;

import java.util.List;
import java.util.stream.Collectors;

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
