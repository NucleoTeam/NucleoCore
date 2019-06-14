package com.synload.nucleo.loader;

import com.synload.nucleo.event.NucleoEvent;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.HashSet;
import java.util.Set;

public class LoadHandler {
    public static <T> Set<Object[]> getMethods(T... clazzez) throws IllegalAccessException, NoSuchMethodException, InvocationTargetException, InstantiationException{
        Set<Object[]> methods = new HashSet<>();
        for ( T clazz: clazzez ) {
            if(clazz instanceof Class) {
                for (Method method : ((Class)clazz).getDeclaredMethods()) {
                    if (method.isAnnotationPresent(NucleoEvent.class)){
                        methods.add(new Object[]{((Class) clazz).getDeclaredConstructor().newInstance(), method});
                    }
                }
            }else if(clazz instanceof Object){
                for (Method method:clazz.getClass().getDeclaredMethods()) {
                    if(method.isAnnotationPresent(NucleoEvent.class)){
                        methods.add(new Object[]{clazz, method});
                    }
                }
            }
        }
        return methods;
    }
}
