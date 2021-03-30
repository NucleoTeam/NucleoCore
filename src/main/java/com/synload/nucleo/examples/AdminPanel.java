package com.synload.nucleo.examples;

import com.synload.nucleo.chain.link.NucleoLink;
import com.synload.nucleo.chain.link.NucleoRequirement;
import com.synload.nucleo.data.NucleoData;
import com.synload.nucleo.event.NucleoClass;

import java.util.HashMap;
import java.util.Map;

@NucleoClass
public class AdminPanel {

    @NucleoRequirement(value = "authorize.admin", acceptPreviousLinks = false)
    @NucleoLink("adminpanel")
    public void adminPanel(NucleoData data){

    }

    @NucleoLink("adminpanel.statistics")
    public void dashboard(NucleoData data){
        data.getObjects().create("stats", new HashMap<String, Object>() {{
            put("PostsPerDay",25);
            put("UsersPerDay",10);
        }});
    }
}
