package com.synload.nucleo.data;

import com.synload.nucleo.interlink.InterlinkProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.beans.IntrospectionException;
import java.beans.Introspector;
import java.beans.PropertyDescriptor;
import java.lang.reflect.AnnotatedType;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.util.*;

public class FindLoop {
    private Stack<Object> seenObjects = new Stack<>();
    public FindLoop(){

    }
    protected static final Logger logger = LoggerFactory.getLogger(FindLoop.class);
    public boolean find(Object obj, String tabs) throws IntrospectionException, InvocationTargetException, IllegalAccessException {
        if(obj==null)
            return false;
        if(!(obj instanceof NucleoData))
            return false;
        if(seenObjects.contains(obj)){
            logger.info(tabs + "FOUND:" + obj.getClass().getName());
            return true;
        }
        seenObjects.add(obj);
        for(PropertyDescriptor field: Introspector.getBeanInfo(obj.getClass()).getPropertyDescriptors()){
            Object innerObject = field.getReadMethod().invoke(obj);
            if(innerObject instanceof Map){
                for(Object mapObject : ((Map) innerObject).entrySet()){
                    if(mapObject instanceof Map.Entry){
                        if(find(((Map.Entry) mapObject).getKey(), tabs+"\t")){
                            logger.info(tabs + obj.getClass().getName());
                            return true;
                        }
                        if(find(((Map.Entry) mapObject).getValue(), tabs+"\t")) {
                            logger.info(tabs + ((Map.Entry) mapObject).getKey() +": " +obj.getClass().getName());
                            return true;
                        }
                    }
                }
            } else if(innerObject instanceof Collection) {
                for (Object innerObjectInnerObject : ((Collection) innerObject)) {
                    if (find(innerObjectInnerObject, tabs + "\t")) {
                        logger.info(tabs + obj.getClass().getName());
                        return true;
                    }
                }
            } else {
                if(find(innerObject, tabs+"\t")){
                    logger.info(tabs + obj.getClass().getName());
                    return true;
                }
            }
        }
        return false;
    }
}
