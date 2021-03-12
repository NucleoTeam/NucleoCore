package com.synload.nucleo.event;

import com.google.common.collect.Sets;
import com.synload.nucleo.NucleoMesh;
import com.synload.nucleo.chain.link.NucleoLinkMeta;
import com.synload.nucleo.hub.Hub;
import com.synload.nucleo.hub.HubEvent;
import com.synload.nucleo.hub.HubEventType;
import com.synload.nucleo.interlink.InterlinkEvent;
import com.synload.nucleo.interlink.InterlinkEventType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;

public class EventHandler {
    protected static final Logger logger = LoggerFactory.getLogger(EventHandler.class);
    private Map<EventType, Map<String, Set<Object[]>>> eventToMethod = new HashMap<>() {{
        put(EventType.INTERLINK, new HashMap<>());
        put(EventType.HUB, new HashMap<>());
    }};


    public void registerInterlink(Object object, Method method, EventType eventType) {
        InterlinkEvent eventData = method.getAnnotation(InterlinkEvent.class);
        String typeValue = eventData.value().getValue();
        if (!eventToMethod.get(eventType).containsKey(typeValue)) {
            eventToMethod.get(eventType).put(typeValue, Sets.newLinkedHashSet());
        }
        eventToMethod.get(eventType).get(typeValue).add(new Object[]{object, method, eventData});
    }

    public void registerHub(Object object, Method method, EventType eventType) {
        HubEvent eventData = method.getAnnotation(HubEvent.class);
        String typeValue = eventData.value().getValue();
        if (!eventToMethod.get(eventType).containsKey(typeValue)) {
            eventToMethod.get(eventType).put(typeValue, Sets.newLinkedHashSet());
        }
        eventToMethod.get(eventType).get(typeValue).add(new Object[]{object, method, eventData});
    }


    private void executeMethod(Set<Object[]> methods, NucleoMesh mesh, Object... data){
        new Thread(() -> {
            methods.stream().forEach(methodObject -> {
                try {
                    Method method = (Method) methodObject[1];
                    Object[] objects = new Object[method.getParameterCount()];
                    Class[] classes = method.getParameterTypes();
                    for (int i = 0; i < classes.length; i++) {
                        if (classes[i] == NucleoMesh.class) {
                            objects[i] = mesh;
                            continue;
                        }
                        if (classes[i] == Hub.class) {
                            objects[i] = mesh.getHub();
                            continue;
                        }
                        Class lookFor = classes[i];
                        Optional<Object> objs = Arrays.stream(data).filter(x->x.getClass()==lookFor).findFirst();
                        if(objs.isPresent()){
                            objects[i] = objs.get();
                            continue;
                        }
                        objects[i] = null;
                    }
                    method.invoke(methodObject[0], objects);
                } catch (IllegalAccessException e) {
                    e.printStackTrace();
                } catch (InvocationTargetException e) {
                    e.printStackTrace();
                }
            });
        }).start();
    }

    public void callHubEvent(HubEventType hubEventType, NucleoMesh mesh, Object... data) {
        if (eventToMethod.get(EventType.HUB).containsKey(hubEventType.getValue())) {
            Set<Object[]> methods = eventToMethod.get(EventType.HUB).get(hubEventType.getValue());
            executeMethod(methods, mesh, data);
        }
    }

    public void callInterlinkEvent(InterlinkEventType interlinkEventType, NucleoMesh mesh, Object... data) {
        if (eventToMethod.get(EventType.INTERLINK).containsKey(interlinkEventType.getValue())) {
            Set<Object[]> methods = eventToMethod.get(EventType.INTERLINK).get(interlinkEventType.getValue());
            executeMethod(methods, mesh, data);
        }
    }

}
