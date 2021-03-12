package com.synload.nucleo.chain.path;

import java.util.HashMap;
import java.util.Map;

public class PathDisplay {
    Map<Integer, Character> build = new HashMap<>();
    char[] chars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789".toCharArray();
    void display(Run run, String spaces){
        if(run.getClass() == ParallelRun.class){
            System.out.println(spaces+"- Parallel");
            run.getNextRuns().forEach(r->display(r, spaces+"  "));
        }else if(run.getClass() == SingularRun.class){
            int x = System.identityHashCode(run);
            if(!build.containsKey(x)){
                build.put(x, chars[build.size()]);
            }
            System.out.println(spaces+"- "+build.get(x)+": "+((SingularRun) run).getChain()+" ["+run.getParents().size()+"] ["+run.isParallel()+"]");
            run.getNextRuns().forEach(r->display(r, spaces+"  "));
        }
    }
}
