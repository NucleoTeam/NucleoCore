package com.synload.nucleo.loader;

import com.synload.nucleo.event.NucleoEvent;
import com.synload.nucleo.hub.Hub;
import java.lang.reflect.Method;
import java.util.HashSet;
import java.util.Set;

public class LoadHandler {

    private ClassLoader classLoader;
    /*private Vector getAllLoadedClasses()
      throws NoSuchFieldException, SecurityException,  IllegalArgumentException, IllegalAccessException {
        Field classesField = ClassLoader.class.getDeclaredField("classes");
        classesField.setAccessible(true);
        return (Vector) classesField.get(classLoader);
    }*/
    public static <T> Set<Object[]> getMethods(T... clazzez) throws IllegalAccessException, InstantiationException{
        Set<Object[]> methods = new HashSet<>();
        for ( T clazz: clazzez ) {
            if(clazz instanceof Class) {
                System.out.println("clazz: "+clazz.getClass().getName());
                for (Method method : ((Class)clazz).getDeclaredMethods()) {
                    if (method.isAnnotationPresent(NucleoEvent.class)) methods.add(new Object[]{((Class) clazz).newInstance(), method});
                }
            }else if(clazz instanceof Object){
                System.out.println("obj: "+clazz.getClass().getName());
                for (Method method:clazz.getClass().getDeclaredMethods()) {
                    if(method.isAnnotationPresent(NucleoEvent.class)) methods.add(new Object[]{clazz, method});
                }
            }
        }
        return methods;
    }
}
