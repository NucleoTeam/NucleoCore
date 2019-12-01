package com.synload.nucleo.data;

import java.util.List;
import java.util.Map;

public class NucleoObjectMap {
    private Map objects = null;
    private String path = "";
    private NucleoObject nucleoObject = null;
    public NucleoObjectMap(Map objects, String path, NucleoObject nucleoObject){
        this.objects = objects;
        this.path = path;
        this.nucleoObject = nucleoObject;
    }

    public Map getObjects() {
        return objects;
    }

    public void setObjects(Map objects) {
        this.objects = objects;
    }

    public String getPath() {
        return path;
    }

    public void setPath(String path) {
        this.path = path;
    }

    public NucleoObject getNucleoObject() {
        return nucleoObject;
    }

    public void setNucleoObject(NucleoObject nucleoObject) {
        this.nucleoObject = nucleoObject;
    }

    public boolean delete(String key){
        return this.nucleoObject.delete(String.join(".", path, key));
    }
    public boolean add(String key, Object obj){
        return this.nucleoObject.add(String.join(".", path, key), obj);
    }
}
