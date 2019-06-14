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

    public Object[] getPrevious(String chainString){
        Set<String> previous = null;
        if(chainString.replaceAll("\\s+","").contains(">")){
            previous = new HashSet<>();
            String[] elements = chainString.replaceAll("\\s+","").split(">");
            int items = elements.length;
            if(items>0) {
                chainString = elements[items - 1];
            }
            for(int x=0;x<items-1;x++){
                previous.add(elements[x]);
            }
        }else{
            chainString = chainString.replaceAll("\\s+","");
        }
        return new Object[]{chainString, previous};
    }

    public String[] registerMethod(Object[] methodData){
        if(methodData.length==2) {
            Object clazz = methodData[0];
            Method method = (Method)methodData[1];
            NucleoEvent nEvent = method.getAnnotation(NucleoEvent.class);
            String chain = nEvent.value();
            if(chain.equals("") && nEvent.chains().length>0){
                String[] chains = nEvent.chains();
                int x=0;
                if(nEvent.chains().length>0){
                    for( String chainString: nEvent.chains()){
                        Object[] elems = getPrevious(chainString);
                        chainToMethod.put( (String)elems[0], new Object[]{ clazz, method, (Set<String>)elems[1] });
                        chains[x]=(String)elems[0];
                        x++;
                    }
                    return chains;
                }
            }else{
                Object[] elems = getPrevious(chain);
                chainToMethod.put( (String)elems[0], new Object[]{ clazz, method, (Set<String>)elems[1] });
                return new String[]{(String)elems[0]};
            }
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
