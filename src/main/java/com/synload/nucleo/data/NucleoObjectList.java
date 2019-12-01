package com.synload.nucleo.data;

import com.google.common.collect.Lists;

import java.util.List;

public class NucleoObjectList {
    private List objects = Lists.newLinkedList();
    private String path = "";
    private NucleoObject nucleoObject = null;
    public NucleoObjectList(List<Object> objects, String path, NucleoObject nucleoObject){
        this.objects = objects;
        this.path = path;
        this.nucleoObject = nucleoObject;
    }

    public List getObjects() {
        return objects;
    }

    public void setObjects(List objects) {
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

    public boolean delete(Integer idx){
        return this.nucleoObject.delete(String.join(".", path, idx.toString()));
    }
    public boolean add(Object obj){
        return this.nucleoObject.add(path, obj, false);
    }
}
