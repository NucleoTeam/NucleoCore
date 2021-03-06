package com.synload.nucleo.event;

import com.google.common.collect.Sets;
import com.synload.nucleo.NucleoMesh;
import com.synload.nucleo.data.NucleoData;
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
    private static Map<EventType, Map<String, Set<Object[]>>> eventToMethod = new HashMap<>() {{
        put(EventType.LINK, new HashMap<>());
        put(EventType.INTERLINK, new HashMap<>());
        put(EventType.HUB, new HashMap<>());
    }};

    public static String SHA256(byte[] convertme) throws NoSuchAlgorithmException {
        byte[] mdbytes = MessageDigest.getInstance("SHA-256").digest(convertme);
        StringBuffer hexString = new StringBuffer();
        for (int i = 0; i < mdbytes.length; i++) {
            hexString.append(Integer.toHexString(0xFF & mdbytes[i]));
        }
        return hexString.toString();
    }

    public Object[] getPrevious(String chainString) {
        Set<String> previous = null;
        if (chainString.replaceAll("\\s+", "").contains(">")) {
            previous = new HashSet<>();
            String[] elements = chainString.replaceAll("\\s+", "").split(">");
            int items = elements.length;
            if (items > 0) {
                chainString = elements[items - 1];
            }
            for (int x = 0; x < items - 1; x++) {
                previous.add(elements[x]);
            }
        } else {
            chainString = chainString.replaceAll("\\s+", "");
        }
        return new Object[]{chainString, previous};
    }

    private void registerLink(Object object, Method method, EventType eventType) {
        NucleoLink nEvent = method.getAnnotation(NucleoLink.class);
        String chain = nEvent.value();
        if (chain.equals("") && nEvent.chains().length > 0) {
            String[] chains = nEvent.chains();
            int x = 0;
            if (nEvent.chains().length > 0) {
                for (String chainString : nEvent.chains()) {
                    Object[] elems = getPrevious(chainString);
                    if (!eventToMethod.get(eventType).containsKey(elems[0])) {
                        eventToMethod.get(eventType).put((String) elems[0], Sets.newLinkedHashSet());
                    }
                    eventToMethod.get(eventType).get(elems[0]).add(new Object[]{object, method, elems[1]});
                    chains[x] = (String) elems[0];
                    x++;
                }
            }
        } else {
            Object[] elems = getPrevious(chain);
            if (!eventToMethod.get(eventType).containsKey(elems[0])) {
                eventToMethod.get(eventType).put((String) elems[0], Sets.newLinkedHashSet());
            }
            eventToMethod.get(eventType).get(elems[0]).add(new Object[]{object, method, elems[1]});
        }
    }

    private void registerInterlink(Object object, Method method, EventType eventType) {
        InterlinkEvent eventData = method.getAnnotation(InterlinkEvent.class);
        String typeValue = eventData.value().getValue();
        if (!eventToMethod.get(eventType).containsKey(typeValue)) {
            eventToMethod.get(eventType).put(typeValue, Sets.newLinkedHashSet());
        }
        eventToMethod.get(eventType).get(typeValue).add(new Object[]{object, method, eventData});
    }

    private void registerHub(Object object, Method method, EventType eventType) {
        HubEvent eventData = method.getAnnotation(HubEvent.class);
        String typeValue = eventData.value().getValue();
        if (!eventToMethod.get(eventType).containsKey(typeValue)) {
            eventToMethod.get(eventType).put(typeValue, Sets.newLinkedHashSet());
        }
        eventToMethod.get(eventType).get(typeValue).add(new Object[]{object, method, eventData});
    }


    private void registerMethod(Object object, Method method, EventType eventType) {
        switch (eventType) {
            case HUB:
                registerHub(object, method, eventType);
                break;
            case INTERLINK:
                registerInterlink(object, method, eventType);
                break;
            case LINK:
                registerLink(object, method, eventType);
                break;
        }
    }

    public <T> void registerClasses(Set<T> classes) throws IllegalAccessException, NoSuchMethodException, InvocationTargetException, InstantiationException {
        for (T clazz : classes) {
            if (clazz instanceof Class) {
                Object obj = ((Class) clazz).getDeclaredConstructor().newInstance();
                for (Method method : ((Class) clazz).getDeclaredMethods()) {
                    Arrays.stream(EventType.values()).forEach(a -> {
                        if (method.isAnnotationPresent(a.clazz)) {
                            logger.info(a.value + "| " + ((Class) clazz).getName() + "->" + method.getName());
                            registerMethod(obj, method, a);
                        }
                    });
                }
            } else if (clazz instanceof Object) {
                for (Method method : clazz.getClass().getDeclaredMethods()) {
                    Arrays.stream(EventType.values()).forEach(a -> {
                        if (method.isAnnotationPresent(a.clazz)) {
                            logger.info(a.value + "| " + ((Object) clazz).getClass().getName() + "->" + method.getName());
                            registerMethod(clazz, method, a);
                        }
                    });
                }
            }
        }
    }

    public Map<String, Set<Object[]>> getChainToMethod() {
        return eventToMethod.get(EventType.LINK);
    }

    public Object[] getChainToMethod(String key) {
        if (eventToMethod.get(EventType.LINK).containsKey(key)) {
            Optional<Object[]> chain = eventToMethod.get(EventType.LINK).get(key).stream().findFirst();
            if (chain.isPresent())
                return chain.get();
        }
        return null;
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
