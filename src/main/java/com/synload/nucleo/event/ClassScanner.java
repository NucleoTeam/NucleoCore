package com.synload.nucleo.event;

import com.synload.nucleo.chain.ChainHandler;
import com.synload.nucleo.chain.path.PathGenerationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Set;

public class ClassScanner {
    protected static final Logger logger = LoggerFactory.getLogger(ClassScanner.class);

    EventHandler eventHandler;
    ChainHandler chainHandler;
    public ClassScanner(EventHandler eventHandler, ChainHandler chainHandler) {
        this.eventHandler = eventHandler;
        this.chainHandler = chainHandler;
    }

    public <T> void registerClasses(Set<T> classes) throws IllegalAccessException, NoSuchMethodException, InvocationTargetException, InstantiationException {
        for (T clazz : classes) {
            if (clazz instanceof Class) {
                Object obj = ((Class) clazz).getDeclaredConstructor().newInstance();
                for (Method method : ((Class) clazz).getDeclaredMethods())
                    Arrays.stream(EventType.values()).filter(a -> method.isAnnotationPresent(a.clazz)).forEach(a -> {
                        try {
                            registerMethod(obj, method, a);
                        } catch (PathGenerationException e) {
                            e.printStackTrace();
                        }
                    });
            } else if (clazz instanceof Object) {
                for (Method method : clazz.getClass().getDeclaredMethods())
                    Arrays.stream(EventType.values()).filter(a -> method.isAnnotationPresent(a.clazz)).forEach(a -> {
                        try {
                            registerMethod(clazz, method, a);
                        } catch (PathGenerationException e) {
                            e.printStackTrace();
                        }
                    });
            }
        }
    }
    private void registerMethod(Object object, Method method, EventType eventType) throws PathGenerationException {
        switch (eventType) {
            case HUB:
                eventHandler.registerHub(object, method);
                break;
            case INTERLINK:
                eventHandler.registerInterlink(object, method);
                break;
            case LINK:
                chainHandler.registerLink(object, method);
                break;
        }
    }
}
