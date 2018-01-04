package com.synload.nucleo.server.socket.reader;

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectStreamClass;

public class ObjectLoader extends ObjectInputStream{
    private ClassLoader classLoader;
    public ObjectLoader(ClassLoader classLoader, InputStream in) throws IOException {
        super(in);
        this.classLoader = classLoader;
    }
    @Override
    protected Class<?> resolveClass(ObjectStreamClass desc) throws IOException, ClassNotFoundException{
        try{
            String name = desc.getName();
            return Class.forName(name, false, classLoader);
        }
        catch(ClassNotFoundException e){
            return super.resolveClass(desc);
        }
    }
}
