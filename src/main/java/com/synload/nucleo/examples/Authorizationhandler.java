package com.synload.nucleo.examples;

import com.synload.nucleo.chain.link.NucleoLink;
import com.synload.nucleo.chain.link.NucleoRequirement;
import com.synload.nucleo.data.NucleoData;
import com.synload.nucleo.event.NucleoClass;

@NucleoClass
public class Authorizationhandler {
    @NucleoRequirement(value = "session.load")
    @NucleoLink(value = "authorize")
    public void getAuthorize(){}

    @NucleoLink(value = "authorize.admin", always = true)
    public void getAuthorizeAdminCheck(NucleoData data){
        Object sessionData = data.getObjects().get("sessionData");
        if(sessionData.getClass() == SessionData.class){
            if(((SessionData) sessionData).getFlags().contains('A')){
                return; // can proceed
            }
        }
        data.breakChain("Not an administrator");
    }

    @NucleoRequirement(value = "session.load")
    @NucleoLink(value = "authorize.user")
    public void getAuthorizeUserCheck(NucleoData data){
        Object sessionData = data.getObjects().get("sessionData");
        if(sessionData.getClass() == SessionData.class){
            if(((SessionData) sessionData).getFlags().contains('U')){
                return; // can proceed
            }
        }
        data.breakChain("Not a user!");
    }

    @NucleoRequirement(value = "authorize.admin", linkOnly = true, acceptPreviousLinks = false)
    @NucleoLink(value = "authorize.admin.create", always = true)
    public void getAuthorizeCreate(NucleoData data){
        Object sessionData = data.getObjects().get("sessionData");
        if(sessionData.getClass() == SessionData.class){
            if(((SessionData) sessionData).getFlags().contains('C')){
                return;// can proceed
            }
        }
        data.breakChain("Not authorized to create.");
    }
    @NucleoRequirement(value = "authorize.admin", linkOnly = true, acceptPreviousLinks = false)
    @NucleoLink(value = "authorize.admin.delete", always = true)
    public void getAuthorizeDelete(NucleoData data){
        Object sessionData = data.getObjects().get("sessionData");
        if(sessionData.getClass() == SessionData.class){
            if(((SessionData) sessionData).getFlags().contains('D')){
                return;// can proceed
            }
        }
        data.breakChain("Not authorized to delete.");
    }
    @NucleoRequirement(value = "authorize.admin", linkOnly = true, acceptPreviousLinks = false)
    @NucleoLink(value = "authorize.admin.update", always = true)
    public void getAuthorizeAdminUpdate(NucleoData data){
        Object sessionData = data.getObjects().get("sessionData");
        if(sessionData.getClass() == SessionData.class){
            if(((SessionData) sessionData).getFlags().contains('D')){
                return;// can proceed
            }
        }
        data.breakChain("Not authorized to update.");
    }

    @NucleoRequirement(value = "authorize.user", linkOnly = true, acceptPreviousLinks = false)
    @NucleoLink(value = "authorize.user.create", always = true)
    public void getAuthorizeUserUpdate(NucleoData data){
        Object sessionData = data.getObjects().get("sessionData");
        if(sessionData.getClass() == SessionData.class){
            if(((SessionData) sessionData).getFlags().contains('D')){
                return;// can proceed
            }
        }
        data.breakChain("Not authorized to update.");
    }
}
