package com.synload.nucleo.data;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.*;
import java.util.Map;

public class NucleoChange implements Serializable {
    private NucleoChangeType type;
    private byte[] object = null;
    private String objectPath;
    private int parallelStep;
    NucleoChangePriority priority = NucleoChangePriority.NONE; // add weight for order merging of data

    @JsonIgnore
    private ObjectMapper mapper = new ObjectMapper(){{
        this.enableDefaultTyping();
        this.setSerializationInclusion(JsonInclude.Include.NON_NULL);
    }};

    public NucleoChange() {
    }

    public NucleoChange(NucleoChangeType type, String objectPath, Object object, int parallelStep, NucleoChangePriority priority) {
        this.type = type;
        this.objectPath = objectPath;
        this.parallelStep = parallelStep;
        this.priority = priority;
        try {
            ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
            ObjectOutputStream objectOutputStream = null;
            try {
                objectOutputStream = new ObjectOutputStream(byteArrayOutputStream);
                objectOutputStream.writeObject(object);
                objectOutputStream.flush();
                this.object = byteArrayOutputStream.toByteArray();
            } catch (IOException e) {
                e.printStackTrace();
            } finally {
                try {
                    if (byteArrayOutputStream != null)
                        byteArrayOutputStream.close();
                }catch (Exception e){
                    e.printStackTrace();
                }
                try {
                    if (objectOutputStream != null)
                        objectOutputStream.close();
                }catch (Exception e){
                    e.printStackTrace();
                }
            }
        }catch (Exception e){
            e.printStackTrace();
        }
    }
    public NucleoChange(NucleoChangeType type, String objectPath, Object object, int parallelStep) {
        this(type, objectPath, object, parallelStep, NucleoChangePriority.NONE);
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
            Object o = null;
            ByteArrayInputStream bis = new ByteArrayInputStream(this.object);
            if(bis!=null) {
                ObjectInput in = null;
                try {
                    in = new ObjectInputStream(bis);
                    o = in.readObject();
                } catch (IOException | ClassNotFoundException e) {
                    e.printStackTrace();
                } finally {
                    try {
                        if (in != null) {
                            in.close();
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                    try {
                        bis.close();
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }
            return o;
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

    public byte[] getObject() {
        return object;
    }

    public void setObject(byte[] object) {
        this.object = object;
    }

    public int getParallelStep() {
        return parallelStep;
    }

    public void setParallelStep(int parallelStep) {
        this.parallelStep = parallelStep;
    }

    public NucleoChangePriority getPriority() {
        return priority;
    }

    public void setPriority(NucleoChangePriority priority) {
        this.priority = priority;
    }
}