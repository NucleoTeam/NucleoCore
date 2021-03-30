package com.synload.nucleo.examples;

import com.synload.nucleo.NucleoMesh;
import com.synload.nucleo.chain.link.NucleoLink;
import com.synload.nucleo.data.NucleoData;
import com.synload.nucleo.event.NucleoClass;
import org.apache.kafka.common.protocol.types.Field;

import java.util.UUID;

@NucleoClass
public class SessionHandler {
    @NucleoLink("session")
    public void getSession(){
        // first do this
        return;
    }

    @NucleoLink("session.create")
    public NucleoData getSessionCreate(NucleoData data){
        Object userAccount = data.getObjects().get("userAccount"); // can be a User class from a shared library
        if(userAccount!=null && userAccount.getClass() == String.class){
            data.getObjects().create("sessionId", UUID.randomUUID());
        }else{
            data.breakChain("Unable to create session, no user information provided.");
        }
        return data;
    }


    @NucleoLink("session.load")
    public NucleoData getSessionLoad(NucleoData data){
        Object sessionId = data.getObjects().get("sessionId");
        if(sessionId!=null && sessionId.getClass() == UUID.class){
            data.getObjects().create("sessionData",new SessionData("davidC", UUID.randomUUID()));
        }else{
            data.breakChain("Unable to load session, no sessionId provided.");
        }
        return data;
    }

    @NucleoLink("session.delete")
    public void getSessionDelete(NucleoData data){
        data.getObjects().delete("sessionData");
        data.getObjects().delete("sessionId");
    }
}
