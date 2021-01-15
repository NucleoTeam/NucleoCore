package com.synload.nucleo.data;

public enum NucleoChangePriority {
    HIGH(3),
    MEDIUM(2),
    LOW(1),
    NONE(0);
    public final int value;
    NucleoChangePriority(int val){
        this.value = val;
    }
}
