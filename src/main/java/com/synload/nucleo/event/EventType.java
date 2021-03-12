package com.synload.nucleo.event;

import com.synload.nucleo.chain.link.NucleoLink;
import com.synload.nucleo.hub.HubEvent;
import com.synload.nucleo.interlink.InterlinkEvent;

import java.util.HashMap;
import java.util.Map;

public enum EventType {
    LINK("link", NucleoLink.class),
    INTERLINK("interlink", InterlinkEvent.class),
    HUB("hub", HubEvent.class);
    public String value;
    public Class clazz;
    private static final Map<String, EventType> valueToLink = new HashMap<>(){{
        put("link", LINK);
        put("interlink", LINK);
        put("hub", LINK);
    }};
    private static final Map<Class, EventType> classToLink = new HashMap<>(){{
        put(NucleoLink.class, LINK);
        put(InterlinkEvent.class, LINK);
        put(HubEvent.class, LINK);
    }};
    EventType(String value, Class clazz){
        this.value = value;
        this.clazz = clazz;
    }
    public static EventType byClass(Class clazz){
        if(classToLink.containsKey(clazz))
            return classToLink.get(clazz);
        return null;
    }
    public static EventType byValue(String value){
        if(valueToLink.containsKey(value))
            return valueToLink.get(value);
        return null;
    }

    public static Map<String, EventType> getValueToLink() {
        return valueToLink;
    }

    public static Map<Class, EventType> getClassToLink() {
        return classToLink;
    }
}
