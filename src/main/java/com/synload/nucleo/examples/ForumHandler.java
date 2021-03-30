package com.synload.nucleo.examples;

import com.synload.nucleo.chain.link.NucleoLink;
import com.synload.nucleo.chain.link.NucleoRequirement;
import com.synload.nucleo.data.NucleoData;

public class ForumHandler {
    @NucleoLink("forum")
    public void forum(){}


    @NucleoLink("forum.list")
    public void forumList(NucleoData data){
        data.getObjects().create("forums", new String[]{"General","Open Discussion"});
    }

    @NucleoRequirement(value = "authorize.admin", acceptPreviousLinks = false, linkOnly = true)
    @NucleoLink("forum.add")
    public void forumAdd(NucleoData data){
        //action to create another forum
        Object newForumObject = data.getObjects().get("forumToAdd");
        if(newForumObject.getClass() == String.class) {
            data.getObjects().create("forums", new String[]{"General", "Open Discussion", (String) newForumObject});
        }
    }
}
