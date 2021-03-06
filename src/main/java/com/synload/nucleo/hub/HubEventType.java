package com.synload.nucleo.hub;

public enum HubEventType {
    DATA_RECEIVED("data_received");
    private String value;
    HubEventType(String value){
        this.value = value;
    }
    public String getValue() {
        return value;
    }
}
