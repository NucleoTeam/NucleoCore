package com.synload.nucleo.data;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.Objects;

public class NucleoChange{
    private NucleoChangeType type;
    private String clazz = null;
    private byte[] object = null;
    private String objectPath;

    @JsonIgnore
    private ObjectMapper mapper = new ObjectMapper(){{
        this.enableDefaultTyping();
        this.setSerializationInclusion(JsonInclude.Include.NON_NULL);
    }};

    public NucleoChange() {
    }

    public NucleoChange(NucleoChangeType type, String objectPath, Object object) {
        this.type = type;
        this.objectPath = objectPath;
        try {
            if(object!=null) {
                this.object = mapper.writeValueAsBytes(object);
                this.clazz = object.getClass().getName();
            }
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    public NucleoChangeType getType() {
        return type;
    }

    public void setType(NucleoChangeType type) {
        this.type = type;
    }

    public Object storedObject(){
        if(this.object==null)
            return null;
        try {
            return mapper.readValue(this.object, Class.forName(this.clazz));
        }catch (Exception e){
            e.printStackTrace();
        }
        return null;
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

    public String getClazz() {
        return clazz;
    }

    public void setClazz(String clazz) {
        this.clazz = clazz;
    }

    public byte[] getObject() {
        return object;
    }

    public void setObject(byte[] object) {
        this.object = object;
    }
}