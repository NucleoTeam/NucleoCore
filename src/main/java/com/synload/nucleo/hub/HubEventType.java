package com.synload.nucleo.hub;

public enum HubEventType {
    DATA_RECEIVED("data_received"),
    CHAIN_META_UPDATED("updated_chain_meta"),
    CHAIN_META_ADDED("added_chain_meta"),
    CHAIN_META_REMOVED("removed_chain_meta");

    private String value;
    HubEventType(String value){
        this.value = value;
    }
    public String getValue() {
        return value;
    }
}
