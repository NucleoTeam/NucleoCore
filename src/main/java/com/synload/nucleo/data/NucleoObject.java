package com.synload.nucleo.data;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Queues;
import org.apache.commons.beanutils.PropertyUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.beans.*;
import java.util.*;

public class NucleoObject {
    private List<NucleoChange> changes = Lists.newLinkedList();
    private Map<String, Object> objects = Maps.newHashMap();

    @JsonIgnore
    private ObjectMapper mapper = new ObjectMapper(){{
        this.enableDefaultTyping();
        this.setSerializationInclusion(JsonInclude.Include.NON_NULL);
    }};

    @JsonIgnore protected static final Logger logger = LoggerFactory.getLogger(NucleoObject.class);


    public NucleoObject() {

    }

    public NucleoObject(NucleoObject old) {
        changes = Lists.newLinkedList(old.changes);
    }

    public NucleoObject latestObjects() {
        objects = Maps.newHashMap();
        Lists.newLinkedList(this.changes).forEach(c->{
            if(c.getType()==NucleoChangeType.SET)
                set(c.getObjectPath(), c.storedObject(), true);
            if(c.getType()==NucleoChangeType.DELETE)
                delete(c.getObjectPath(), true);
            if(c.getType()==NucleoChangeType.ADD)
                add(c.getObjectPath(), c.storedObject(), true);
        });
        return this;
    }

    public Map<String, Object> getObjects() {
        return objects;
    }

    public void setObjects(Map<String, Object> objects) {
        this.objects = objects;
    }

    public Queue splitPath(String objectPath){
        return Queues.newArrayDeque(Arrays.asList(objectPath.split("\\.")));
    }

    // GET DATA USING PATH
    public Object get(String objectPath){
        return get( splitPath(objectPath), this.objects );
    }
    public Object get(Queue<String> objectPath, Object obj){
        if(objectPath.isEmpty() || obj == null)
            return obj;
        String objectKey = objectPath.poll();
        try {
            if(List.class.isInstance(obj)){
                return get(objectPath, ((List)obj).get(Integer.valueOf(objectKey)));
            }
            if(Map.class.isInstance(obj)){
                if(!((Map)obj).containsKey(objectKey))
                    return null;
                return get(objectPath, ((Map)obj).get(objectKey));
            }
            Object nextObj = PropertyUtils.getProperty(obj, objectKey);
            //logger.info(new ObjectMapper().writeValueAsString(nextObj));
            return get(objectPath, nextObj);
        }catch (Exception e){
            e.printStackTrace();
            return null;
        }
    }

    // Add to value based on path/ only works for lists
    public boolean add(String objectPath, Object objectToAdd){
        Object obj = get(objectPath);
        if(List.class.isInstance(obj)) {
            ((List) obj).add(objectToAdd);
            return true;
        }
        return false;
    }
    public boolean add(String objectPath, Object objectToAdd, boolean latest){
        if(!latest)
            changes.add(new NucleoChange(NucleoChangeType.ADD, objectPath, objectToAdd));
        return add(objectPath, objectToAdd);
    }


    public boolean set(String objectPath, Object objectToSave){
        return set(objectPath, objectToSave, false);
    }
    public boolean set(String objectPath, Object objectToSave, boolean latest){
        if(!latest)
            changes.add(new NucleoChange(NucleoChangeType.SET, objectPath, objectToSave));
        return set( splitPath(objectPath), this.objects, objectToSave);
    }
    public boolean set(Queue<String> objectPath, Object obj, Object objectToSave){
        boolean save = false;
        if(objectPath.isEmpty())
            return false;
        String objectKey = objectPath.poll();
        if(objectPath.isEmpty())
            save = true;
        try {
            if(List.class.isInstance(obj)){
                if(save) {
                    ((List) obj).set(Integer.valueOf(objectKey), objectToSave);
                    return true;
                }else
                    return set(objectPath, ((List)obj).get(Integer.valueOf(objectKey)), objectToSave);
            }
            if(Map.class.isInstance(obj)){
                if(save) {
                    ((Map)obj).put(objectKey, objectToSave);
                    return true;
                }else
                    return set(objectPath, ((Map)obj).get(objectKey), objectToSave);
            }
            if(save){
                new PropertyDescriptor(objectKey,obj.getClass()).getWriteMethod().invoke(obj, objectToSave);
                return true;
            }else {
                Object nextObj = PropertyUtils.getProperty(obj, objectKey);
                return set(objectPath, nextObj, objectToSave);
            }
        }catch (Exception e){
            e.printStackTrace();
        }
        return false;
    }

    public boolean delete(String objectPath){
        return delete(objectPath, false);
    }
    public boolean delete(String objectPath, boolean latest){
        if(!latest)
            changes.add(new NucleoChange(NucleoChangeType.DELETE, objectPath, null));
        return delete( splitPath(objectPath), this.objects);
    }
    public boolean delete(Queue<String> objectPath, Object obj){
        boolean delete = false;
        if(objectPath.isEmpty())
            return false;
        Object objectKey = objectPath.poll();
        if(objectPath.isEmpty())
            delete = true;
        try {
            if(List.class.isInstance(obj)){
                if(delete) {
                    if(Integer.class.isInstance(objectKey)) {
                        ((List) obj).remove(((Integer)objectKey).intValue());
                        return true;
                    }else if(int.class.isInstance(objectKey)) {
                        ((List) obj).remove((int)objectKey);
                        return true;
                    }else if(String.class.isInstance(objectKey)) {
                        ((List) obj).remove(Integer.valueOf((String)objectKey).intValue());
                        return true;
                    }else
                        return false;
                }else {
                    if (Integer.class.isInstance(objectKey)) {
                        return delete(objectPath, ((List) obj).get(((Integer)objectKey).intValue()));
                    } else if (int.class.isInstance(objectKey)) {
                        return delete(objectPath, ((List) obj).get((int) objectKey));
                    } else if (String.class.isInstance(objectKey)) {
                        return delete(objectPath, ((List) obj).get(Integer.valueOf((String) objectKey).intValue()));
                    } else
                        return false;
                }
            }
            if(Map.class.isInstance(obj)){
                if(delete) {
                    if(!((Map)obj).containsKey(objectKey))
                        return false;
                    ((Map)obj).remove(objectKey);
                    return true;
                }else {
                    if(!((Map)obj).containsKey(objectKey))
                        return false;
                    return delete(objectPath, ((Map) obj).get(objectKey));
                }
            }
            if(delete){
                if(String.class.isInstance(objectKey)) {
                    new PropertyDescriptor((String) objectKey, obj.getClass()).getWriteMethod().invoke(obj);
                    return true;
                }
                return false;
            }else {
                if (String.class.isInstance(objectKey)) {
                    return delete(objectPath, new PropertyDescriptor((String)objectKey, obj.getClass()).getReadMethod().invoke(obj));
                } else
                    return false;
            }
        }catch (Exception e){
            e.printStackTrace();
        }
        return false;
    }
    public NucleoObjectMap map(String objectPath){
        Object obj = get(objectPath);
        if(Map.class.isInstance(obj))
            return new NucleoObjectMap((Map)obj, objectPath, this);
        return null;
    }
    public NucleoObjectList list(String objectPath){
        Object obj = get(objectPath);
        if(List.class.isInstance(obj))
            return new NucleoObjectList((List)obj, objectPath, this);
        return null;
    }
    public boolean exists(String objectPath){
        return get(objectPath) != null;
    }

    public List<NucleoChange> getChanges() {
        return changes;
    }

    public void setChanges(List<NucleoChange> changes) {
        this.changes = changes;
    }
}
