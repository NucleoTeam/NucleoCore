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
    public static Set<Object[]> getMethods(Class... clazzez){
        Set<Object[]> methods = new HashSet<>();
        for ( Class clazz: clazzez ) {
            for (Method method:clazz.getMethods()) {
                if(method.isAnnotationPresent(NucleoEvent.class)) methods.add(new Object[]{clazz, method});
            }
        }
        return methods;
    }
}
