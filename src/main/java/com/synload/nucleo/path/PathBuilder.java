package com.synload.nucleo.path;

import java.util.*;

public class PathBuilder {
    Run start = null;
    List<Run> currentLeafs = new LinkedList<>();
    private class TestRequirements{
        private HashMap<String, List<String>> requirements = new HashMap<>();
        private HashMap<String, Run> requirementFulfillment = new HashMap<>();
    }
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
    PathPlusLeafs generateSerialRun(Object... chains){
        PathPlusLeafs pathPlusLeafs = new PathPlusLeafs();
        List<Run> last = new LinkedList<>();
        for (int i = 0; i < chains.length; i++) {
            if(chains[i].getClass() == PathPlusLeafs.class){
                PathPlusLeafs tmpPathLeaf = (PathPlusLeafs) chains[i];
                Run tmp = tmpPathLeaf.getRoot();
                if (pathPlusLeafs.getRoot() == null)
                    pathPlusLeafs.setRoot(tmp);
                if (last.size()!=0) {
                    tmp.getParents().addAll(last);
                    last.forEach(r->r.getNextRuns().addAll(Arrays.asList(tmp)));
                    last = tmpPathLeaf.getLeafs();
                } else {
                    last = tmpPathLeaf.getLeafs();
                }
            }else if(chains[i].getClass() == String.class) {
                String[] links = ((String)chains[i]).split("\\.");
                String tmpChain = "";
                for (int x = 0; x < links.length; x++) {
                    tmpChain = tmpChain + ((tmpChain.equals("")) ? "" : ".") + links[x];
                    Run tmp = new SingularRun(tmpChain);
                    if (pathPlusLeafs.getRoot() == null)
                        pathPlusLeafs.setRoot(tmp);
                    if (last.size()!=0) {
                        tmp.getParents().addAll(last);
                        last.forEach(r->r.getNextRuns().addAll(Arrays.asList(tmp)));
                        last.clear();
                        last.add(tmp);
                    } else {
                        last.clear();
                        last.add(tmp);
                    }
                }
            }
        }
        pathPlusLeafs.getLeafs().addAll(last);
        return pathPlusLeafs;
    }
    PathPlusLeafs generateRun(String chain){
        return generateSerialRun(chain);
    }
    PathPlusLeafs generateParallelRun(PathPlusLeafs... pathPlusLeafsOld){
        PathPlusLeafs pathPlusLeafs = new PathPlusLeafs();
        ParallelRun parallelRun = new ParallelRun();
        for (int i = 0; i < pathPlusLeafsOld.length; i++) {
            pathPlusLeafsOld[i].getRoot().getParents().add(parallelRun);
            parallelRun.getNextRuns().add(pathPlusLeafsOld[i].getRoot());
            pathPlusLeafs.getLeafs().addAll(pathPlusLeafsOld[i].getLeafs());
        }
        pathPlusLeafs.setRoot(parallelRun);
        return pathPlusLeafs;
    }

    PathBuilder addParallel(PathPlusLeafs... pathPlusLeafsOld){
        PathPlusLeafs pathPlusLeafs = generateParallelRun(pathPlusLeafsOld);
        Run.traverseModify(pathPlusLeafs.getRoot(), (run)->{
           run.setParallel(true);
        });
        if(currentLeafs.size()>0){
            currentLeafs.forEach(leaf->{
                pathPlusLeafs.getRoot().getParents().addAll(currentLeafs);
                leaf.getNextRuns().add(pathPlusLeafs.getRoot());
            });
        }
        currentLeafs = pathPlusLeafs.getLeafs();

        if(start==null)
            start = pathPlusLeafs.getRoot();

        return this;
    }
    PathBuilder add(Object... chains){
        PathPlusLeafs pathPlusLeafs = generateSerialRun(chains);
        if (currentLeafs.size() > 0) {

            currentLeafs.forEach(leaf ->{
                pathPlusLeafs.getRoot().getParents().addAll(currentLeafs);
                leaf.getNextRuns().add(pathPlusLeafs.getRoot());
            });
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
    static PathBuilder fromString(String request){
        PathBuilder pathBuilder = new PathBuilder();
        return pathBuilder;
    }
    static Map<Integer, Character> build = new HashMap<>();
    static char[] chars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789".toCharArray();
    static void display(Run run, String spaces){
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
    //String possibleString = "{[information.hits,information.test],popcorn,{[information.hits,information.test],information.test}},information.test,information.popcorn,{[information.hits,information.test],popcorn}";
    public static void main(String[] args){
        PathBuilder pathBuilder = new PathBuilder();
        pathBuilder.addParallel(
            pathBuilder.generateSerialRun("information.hits","information.test"),
            pathBuilder.generateRun("popcorn"),
            pathBuilder.generateSerialRun(
                "information.hits",
                "information.test",
                pathBuilder.generateParallelRun(
                    pathBuilder.generateSerialRun("test.poppy", "poggers"),
                    pathBuilder.generateSerialRun("you.are.my.little", "pog.champ")
                )
            ),
            pathBuilder.generateRun("information.test")
        ).add(
            "information.test",
            "information.popcorn",
            pathBuilder.generateParallelRun(
                pathBuilder.generateSerialRun("test.poppy", "poggers"),
                pathBuilder.generateSerialRun("you.are.my.little", "pog.champ")
            )
        ).addParallel(
            pathBuilder.generateSerialRun("information.hits","information.test"),
            pathBuilder.generateRun("popcorn")
        ).add(
            "koko"
        );

        List<Run> run = Run.traverseFilter(pathBuilder.getStart(), (r)->{
            if(r.getClass() == SingularRun.class){
                return ((SingularRun) r).getChain().equals("koko");
            }
            return false;
        });

        Run splicer = pathBuilder.generateSerialRun("losing.it").getRoot();
        run.forEach(r->r.splice(splicer.clone()));
        run.forEach(r->r.splice(splicer.clone()));
        run.forEach(r ->display(r, ""));
        run.forEach(r ->r.allParents().forEach(p->{
            if(p.getClass() == SingularRun.class)
                System.out.println("parent: "+((SingularRun) p).getChain());
        }));

        pathBuilder.getStart().last().forEach(r ->{
            if(r.getClass() == SingularRun.class)
                System.out.println("last: "+ ((SingularRun) r).getChain());
        });

        System.out.println("Root");
        display(pathBuilder.getStart(),"");
    }
}
