package com.synload.nucleo.loader;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.List;

public class EventClassLoader extends ClassLoader {

    public static Thread checkNewJar=null;
    public static Hashtable<String, Class<?>> cache = new Hashtable<String, Class<?>>();
    public static HashMap<String, String> loadedModules = new HashMap<String, String>();
    public static List<String> modules = new ArrayList<>();

    public EventClassLoader(ClassLoader parent) {
        super(parent);
    }
    public synchronized Class<?> loadClass(String clazzName) {
        Class<?> c = cache.get(clazzName);
        if (c == null) {
            try {
                c = Class.forName(clazzName);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return c;
    }
    public void loadClass(String clazzName, byte[] clazzBytes) {
        try {
            Class.forName(clazzName);
        } catch (Exception e) {
            Class<?> c = defineClass(clazzName, clazzBytes, 0, clazzBytes.length);
            cache.put(clazzName, c);
        }
    }
    public static String SHA256(byte[] convertme) throws NoSuchAlgorithmException{
        byte[] mdbytes = MessageDigest.getInstance("SHA-256").digest(convertme);
        StringBuffer hexString = new StringBuffer();
        for (int i=0;i<mdbytes.length;i++) {
            hexString.append(Integer.toHexString(0xFF & mdbytes[i]));
        }
        return hexString.toString();
    }
    public static boolean addClassByteArray(String className, byte[] buffer){
        EventClassLoader ml = new EventClassLoader(Thread.currentThread().getContextClassLoader());
        ml.loadClass(className, buffer);
        return false;
    }
}
