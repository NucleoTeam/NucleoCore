package com.synload.nucleo.chain.path;

import java.util.*;

public class PathBuilder {
    Run start = null;
    List<Run> currentLeafs = new LinkedList<>();
    private static class PathRequirements{
        private HashMap<String, List<String>> requirements = new HashMap<>();
        private HashMap<String, Run> requirementFulfillment = new HashMap<>();

        public PathRequirements() {
        }

        public HashMap<String, List<String>> getRequirements() {
            return requirements;
        }

        public void setRequirements(HashMap<String, List<String>> requirements) {
            this.requirements = requirements;
        }

        public HashMap<String, Run> getRequirementFulfillment() {
            return requirementFulfillment;
        }

        public void setRequirementFulfillment(HashMap<String, Run> requirementFulfillment) {
            this.requirementFulfillment = requirementFulfillment;
        }
    }
    public static class PathPlusLeafs{
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
    public static PathPlusLeafs generateSerialRun(Object... chains){
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
    public static PathPlusLeafs generateRun(String chain){
        return generateSerialRun(chain);
    }
    public static PathPlusLeafs generateExactRun(String chain){
        PathPlusLeafs pathPlusLeafs = new PathPlusLeafs();
        Run run = new SingularRun(chain);
        pathPlusLeafs.setRoot(run);
        pathPlusLeafs.getLeafs().add(run);
        return pathPlusLeafs;
    }
    public static PathPlusLeafs generateParallelRun(PathPlusLeafs... pathPlusLeafsOld){
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

    public PathBuilder addParallel(PathPlusLeafs... pathPlusLeafsOld){
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
    public PathBuilder add(Object... chains){
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

    //String possibleString = "{[information.hits,information.test],popcorn,{[information.hits,information.test],information.test}},information.test,information.popcorn,{[information.hits,information.test],popcorn}";
    public static void main(String[] args){
        PathBuilder pathBuilder = new PathBuilder();
        pathBuilder.addParallel(
            generateSerialRun("user.login.check", generateExactRun("user.account.information")),
            generateRun("forum.get.index")
        ).add(
            generateExactRun("forum.get.posts")
        ).add(
            generateExactRun("forum.update.post")
        );

        List<Run> run = Run.traverseFilter(pathBuilder.getStart(), (r)->{
            if(r.getClass() == SingularRun.class){
                return ((SingularRun) r).getChain().equals("forum.get.updates");
            }
            return false;
        });
        PathRequirements testRequirements = new PathRequirements();
        testRequirements.getRequirements().put("forum.get.updates", new LinkedList<>(){{
            add("user.perm.post.update");
        }});
        testRequirements.getRequirementFulfillment().put("forum.get.updates", null);

        Run splicer = generateSerialRun("losing.it", "another.one").getRoot();
        run.forEach(r->r.splice(splicer.clone()));
        run.forEach(r->r.splice(splicer.clone()));
        run.forEach(r ->new PathDisplay().display(r, ""));
        run.forEach(r ->r.allParentsString().forEach(p->System.out.println("parent: "+p)));

        pathBuilder.getStart().last().forEach(r ->{
            if(r.getClass() == SingularRun.class)
                System.out.println("last: "+ ((SingularRun) r).getChain());
        });

        System.out.println("Root");
        new PathDisplay().display(pathBuilder.getStart(),"");
    }
}
