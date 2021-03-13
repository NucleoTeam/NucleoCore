package com.synload.nucleo.chain.path;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.common.collect.Queues;
import org.apache.commons.lang3.SerializationException;
import java.io.*;
import java.util.*;
import java.util.stream.Collectors;

public class Run implements Serializable{
    List<Run> nextRuns = new LinkedList<>();

    @JsonIgnore
    Set<Run> parents = new HashSet<>();

    boolean parallel = false;
    public Set<Run> allParents(){
        Set<Run> allParents = new HashSet<>();
        Queue<Run> parentQueue = Queues.newLinkedBlockingDeque();
        parentQueue.addAll(this.parents);
        while(!parentQueue.isEmpty()){
            Run p = parentQueue.poll();
            allParents.addAll(p.getParents());
            parentQueue.addAll(p.getParents());
        }
        return new HashSet<>(allParents);
    }
    public Set<String> allParentsString(){
        Set<String> allParents = new HashSet<>();
        Queue<Run> parentQueue = Queues.newLinkedBlockingDeque();
        parentQueue.addAll(this.parents);
        while(!parentQueue.isEmpty()){
            Run par = parentQueue.poll();
            allParents.addAll(par.getParents().stream().filter(p->p.getClass()==SingularRun.class).map(p->((SingularRun) p).getChain()).collect(Collectors.toList()));
            parentQueue.addAll(par.getParents());
        }
        return new HashSet<>(allParents);
    }
    public Set<Run> last(){
        List<Run> lastRuns = new LinkedList<>();
        this.getNextRuns().forEach(r->{
            if(r.getNextRuns().isEmpty()){
                lastRuns.add(r);
            }else{
                lastRuns.addAll(r.last());
            }
        });
        return new HashSet<>(lastRuns);
    }
    interface Modification{
        void action(Run run);
    }
    interface Filter{
        boolean filter(Run run);
    }
    static List<Run> traverseFilter(Run start, Filter filterMethod){
        List<Run> filteredRuns = new LinkedList<>();
        Stack<Run> toGo = new Stack();
        HashSet<Run> seen = new HashSet();
        toGo.add(start);
        while(!toGo.isEmpty()){
            Run run = toGo.pop();
            if(filterMethod.filter(run)){
                filteredRuns.add(run);
            }
            run.getNextRuns().forEach(r->{
                if(!seen.contains(r)){
                    toGo.add(r);
                    seen.add(r);
                }
            });
        }
        return filteredRuns;
    }
    static void traverseModify(Run run, Modification modification){
        modification.action(run);
        run.getNextRuns().forEach(i->traverseModify(i, modification));
    }
    public void splice(Run spliced){

        if(this.getParents().stream().filter(r->r.isParallel()).count()>0 && this.isParallel()){
            traverseModify(spliced, r->r.setParallel(true));
        }

        parents.forEach(p->{
            p.getNextRuns().remove(this);
            p.getNextRuns().add(spliced);
        });
        spliced.getParents().addAll(parents);
        this.getParents().clear();

        Set<Run> splicedLastRuns = spliced.last();
        spliced.last().forEach(last->{
            last.getNextRuns().add(this);
        });
        this.parents.addAll(splicedLastRuns);

    }

    public List<Run> getNextRuns() {
        return nextRuns;
    }

    public void setNextRuns(List<Run> nextRuns) {
        this.nextRuns = nextRuns;
    }

    public boolean isParallel() {
        return parallel;
    }

    public void setParallel(boolean parallel) {
        this.parallel = parallel;
    }

    public Set<Run> getParents() {
        return parents;
    }

    public void setParents(Set<Run> parents) {
        this.parents = parents;
    }

    protected Run clone() {
        Run run = null;
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        ObjectOutputStream out = null;
        ByteArrayInputStream bis;
        ObjectInput in = null;
        try {
            out = new ObjectOutputStream(bos);
            out.writeObject(this);
            out.flush();
            bis = new ByteArrayInputStream(bos.toByteArray());
            in = new ObjectInputStream(bis);
            run = (Run) in.readObject();
        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
            throw new SerializationException("Failed to serialize data", e);
        } finally {
            try {
                bos.close();
            } catch (IOException ex) {
                // ignore close exception
            }
            try {
                if (in != null) {
                    in.close();
                }
            } catch (IOException ex) {
                // ignore close exception
            }
        }
        return run;
    }
}
