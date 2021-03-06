package com.synload.nucleo.interlink;

public enum InterlinkEventType {
    GAIN_LEADER("gain_leader"),
    CEDE_LEADER("cede_leader"),
    LEAVE_SERVICE("left_service"),
    NEW_SERVICE("new_service"),
    RECEIVE_TOPIC("receive_topic"),
    SEND_TOPIC("send_topic");
    String value;
    InterlinkEventType(String value){
        this.value = value;
    }

    public String getValue() {
        return value;
    }
}
