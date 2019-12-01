package com.synload.nucleo.data;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.Objects;

public class NucleoChange{
    private NucleoChangeType type;
    private Object object;
    private String objectPath;

    @JsonIgnore
    private ObjectMapper mapper = new ObjectMapper();

    public NucleoChange() {
    }

    public NucleoChange(NucleoChangeType type, String objectPath, Object object) {
        this.type = type;
        this.object = object;
        this.objectPath = objectPath;
    }

    public NucleoChangeType getType() {
        return type;
    }

    public void setType(NucleoChangeType type) {
        this.type = type;
    }

    public Object getObject() {
        return object;
    }

    public void setObject(Object object) {
        this.object = object;
    }

    public String getObjectPath() {
        return objectPath;
    }

    public void setObjectPath(String objectPath) {
        this.objectPath = objectPath;
    }

    @Override
    public boolean equals(Object o) {
        try {
            return mapper.writeValueAsString(this).equals(mapper.writeValueAsString(o));
        }catch (Exception e){
            e.printStackTrace();
        }
        return false;
    }
}