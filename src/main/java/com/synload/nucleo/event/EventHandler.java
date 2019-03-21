package com.synload.nucleo.event;

import java.lang.reflect.Method;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;

public class EventHandler {

    private TreeMap<String, Object[]> chainToMethod = new TreeMap<>();

    public static String SHA256(byte[] convertme) throws NoSuchAlgorithmException{
        byte[] mdbytes = MessageDigest.getInstance("SHA-256").digest(convertme);
        StringBuffer hexString = new StringBuffer();
        for (int i=0;i<mdbytes.length;i++) {
            hexString.append(Integer.toHexString(0xFF & mdbytes[i]));
        }
        return hexString.toString();
    }


    public String registerMethod(Object[] methodData){
        if(methodData.length==2) {
            Object clazz = methodData[0];
            Method method = (Method)methodData[1];
            String chain = method.getAnnotation(NucleoEvent.class).value();
            chainToMethod.put(chain, new Object[]{ clazz, method });
            return chain;
        }
        return null;
    }

    public TreeMap<String, Object[]> getChainToMethod() {
        return chainToMethod;
    }

    public void setChainToMethod(TreeMap<String, Object[]> chainToMethod) {
        this.chainToMethod = chainToMethod;
    }
}