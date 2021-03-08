package com.synload.nucleo.path;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

public class PathBuilder {
    Run start = null;
    List<Run> currentLeafs = new LinkedList<>();
    private class PathPlusLeafs{
        List<Run> leafs = new LinkedList<>();
        Run root = null;
        public PathPlusLeafs() {
        }

        public List<Run> getLeafs() {
            return leafs;
        }

        public void setLeafs(List<Run> leafs) {
            this.leafs = leafs;
        }

        public Run getRoot() {
            return root;
        }

        public void setRoot(Run root) {
            this.root = root;
        }
    }
    /*List<Run> getLeafs(List<Run> runGraph){
        PathPlusLeafs re = new PathPlusLeafs();
        runGraph.stream().forEach(run->{
            if(run.getClass() == ParallelRun.class){
                leafs.addAll(getLeafs(run.getNextRuns()));
            }else if(run.getClass() == SingularRun.class){
                if(run.getNextRuns().size()==0){
                    run.getNextRuns().addAll(next);
                }else{
                    getLeafs(run.getNextRuns());
                }
            }
        });
        return leafs;
    }*/
    PathPlusLeafs generateSerialRun(String... chains){
        PathPlusLeafs pathPlusLeafs = new PathPlusLeafs();
        Run last = null;
        for (int i = 0; i < chains.length; i++) {
            String[] links = chains[i].split("\\.");
            String tmpChain = "";
            for (int x = 0; x < links.length; x++) {
                tmpChain = tmpChain+((tmpChain.equals(""))?"":".")+links[x];
                Run tmp = new SingularRun(tmpChain);
                if(pathPlusLeafs.getRoot()==null)
                    pathPlusLeafs.setRoot(tmp);
                if(last!=null) {
                    last.getNextRuns().addAll(Arrays.asList(tmp));
                    last = tmp;
                }else{
                    last = tmp;
                }
            }
        }
        pathPlusLeafs.getLeafs().add(last);
        return pathPlusLeafs;
    }
    PathPlusLeafs generateRun(String chain){
        return generateSerialRun(chain);
    }
    interface Modification{
        void write(Run run);
    }
    void traverseModify(Run run, Modification modification){
        modification.write(run);
        run.getNextRuns().forEach(i->traverseModify(i, modification));
    }
    PathPlusLeafs generateParallelRun(PathPlusLeafs... pathPlusLeafsOld){
        PathPlusLeafs pathPlusLeafs = new PathPlusLeafs();
        ParallelRun parallelRun = new ParallelRun();
        for (int i = 0; i < pathPlusLeafsOld.length; i++) {
            parallelRun.getNextRuns().add(pathPlusLeafsOld[i].getRoot());
            pathPlusLeafs.getLeafs().addAll(pathPlusLeafsOld[i].getLeafs());
        }
        pathPlusLeafs.setRoot(parallelRun);
        return pathPlusLeafs;
    }
    PathBuilder addParallel(PathPlusLeafs... pathPlusLeafsOld){
        PathPlusLeafs pathPlusLeafs = generateParallelRun(pathPlusLeafsOld);
        traverseModify(pathPlusLeafs.getRoot(), (run)->{
           run.setParallel(true);
        });
        if(currentLeafs.size()>0){
            currentLeafs.forEach(leaf->{
                leaf.getNextRuns().add(pathPlusLeafs.getRoot());
            });
        }
        currentLeafs = pathPlusLeafs.getLeafs();

        if(start==null)
            start = pathPlusLeafs.getRoot();

        return this;
    }
    PathBuilder add(String... chains){
        PathPlusLeafs pathPlusLeafs = generateSerialRun(chains);
        if (currentLeafs.size() > 0) {
            currentLeafs.forEach(leaf -> leaf.getNextRuns().add(pathPlusLeafs.getRoot()));
        }
        if (start == null)
            start = pathPlusLeafs.getRoot();
        currentLeafs = pathPlusLeafs.getLeafs();
        return this;
    }
    public Run getStart() {
        return start;
    }

    public void setStart(Run root) {
        this.start = root;
    }

    static void display(Run run, String spaces){
        if(run.getClass() == ParallelRun.class){
            System.out.println(spaces+"- Parallel");
            run.getNextRuns().forEach(r->display(r, spaces+"  "));
        }else if(run.getClass() == SingularRun.class){
            System.out.println(spaces+"- "+((SingularRun) run).getChain()+" [ "+System.identityHashCode(run)+" ] [ "+run.isParallel()+" ]");
            run.getNextRuns().forEach(r->display(r, spaces+"  "));
        }
    }

    public static void main(String[] args){
        PathBuilder pathBuilder = new PathBuilder();
        pathBuilder.addParallel(
            pathBuilder.generateSerialRun("information.hits","information.test"),
            pathBuilder.generateRun("popcorn"),
            pathBuilder.generateSerialRun("information.jest","information.test"),
            pathBuilder.generateSerialRun("information.test")
        ).add(
            "information.test",
            "information.popcorn"
        ).addParallel(
            pathBuilder.generateSerialRun("information.hits","information.test"),
            pathBuilder.generateRun("popcorn")
        );
        System.out.println("Root");
        display(pathBuilder.getStart(),"");
    }


}
