package com.synload.nucleo.chain.path;

import com.synload.nucleo.chain.link.NucleoLinkMeta;
import com.synload.nucleo.chain.link.NucleoMetaHandler;
import com.synload.nucleo.interlink.InterlinkConsumer;
import org.jgrapht.Graph;
import org.jgrapht.graph.DefaultDirectedGraph;
import org.jgrapht.graph.DefaultEdge;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;

public class PathBuilder {
    protected static final Logger logger = LoggerFactory.getLogger(PathBuilder.class);
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
    public class SimpleEdge extends DefaultEdge{
        @Override
        public String toString() {
            return "Next";
        }
    }
    public class VertexChain{
        String chain;

        public VertexChain(String chain) {
            this.chain = chain;
        }

        public String getChain() {
            return chain;
        }

        public void setChain(String chain) {
            this.chain = chain;
        }

        @Override
        public String toString() {
            return chain;
        }
    }
    public void remove(Run run){
        if(!run.getParents().isEmpty()
            && !run.getNextRuns().isEmpty()
            && run.getParents().stream().filter(p->p.getClass() == ParallelRun.class).count()>0
            && run.getNextRuns().stream().filter(c->c.isParallel()).count()==0){
            run.getNextRuns().forEach(c -> {
                c.getParents().remove(run);
            });
            run.getParents().forEach(p -> {
                p.getNextRuns().remove(run);
            });
        }else {
            run.getNextRuns().forEach(c -> {
                c.getParents().remove(run);
                c.getParents().addAll(run.getParents());
            });
            run.getParents().forEach(p -> {
                p.getNextRuns().remove(run);
                p.getNextRuns().addAll(run.getNextRuns());
            });
        }
    }
    public void removeDuplicates(Run current, Set<String> previous, NucleoMetaHandler linkMetaMap){
        if(current.getClass() == SingularRun.class){
            String chain = ((SingularRun) current).getChain();
            if(previous.contains(chain)){
                if(!current.getNextRuns().isEmpty()
                    && !current.getParents().isEmpty()
                    && current.getParents().stream().filter(p->p.isParallel()).count()>0
                    && current.getNextRuns().stream().filter(c->c.isParallel()).count()>0 && !current.isParallel()){

                } else {
                    if(!current.isAlways()){
                        if(linkMetaMap.getNucleoLinkMetaMap().containsKey(chain)){
                            NucleoLinkMeta nucleoLinkMeta = linkMetaMap.getNucleoLinkMetaMap().get(chain);
                            logger.info("NucleoMetaMap lookup for "+chain);
                            if(!nucleoLinkMeta.isAlways()){
                                remove(current);
                            }
                        }else {
                            remove(current);
                        }
                    }
                }
            }else{
                if(linkMetaMap.getNucleoLinkMetaMap().containsKey(chain)){
                    NucleoLinkMeta nucleoLinkMeta = linkMetaMap.getNucleoLinkMetaMap().get(chain);
                    if(nucleoLinkMeta.isBinder()){
                        remove(current);
                    }
                }
                previous.add(chain);
            }
        }
        current.getNextRuns().stream().collect(Collectors.toList()).forEach(r->removeDuplicates(r, new HashSet<>(){{addAll(previous);}}, linkMetaMap));
    }
    public void removeDuplicates(Run current, Set<String> previous){
        if(current.getClass() == SingularRun.class){
            String chain = ((SingularRun) current).getChain();
            if(previous.contains(chain)){
                if(!current.isAlways()) {
                    if (!current.getNextRuns().isEmpty()
                        && !current.getParents().isEmpty()
                        && current.getParents().stream().filter(p -> p.isParallel()).count() > 0
                        && current.getNextRuns().stream().filter(c -> c.isParallel()).count() > 0 && !current.isParallel()) {

                    } else {
                        remove(current);
                    }
                }
            }else{
                previous.add(chain);
            }
        }
        current.getNextRuns().stream().collect(Collectors.toList()).forEach(r->removeDuplicates(r, new HashSet<>(){{addAll(previous);}}));
    }
    public void addRequirements(Run run, NucleoMetaHandler linkMetaMap, Set<String> previous){
        if(run.getClass() == SingularRun.class){
            String chain = ((SingularRun) run).getChain();
            if(linkMetaMap.getNucleoLinkMetaMap().containsKey(chain)){
                NucleoLinkMeta nucleoLinkMeta = linkMetaMap.getNucleoLinkMetaMap().get(chain);
                nucleoLinkMeta.getRequirements().stream().filter(r->r.isAlways() || !(previous.contains(r.getChain()) && r.isAcceptPreviousLinks())).forEach(r->{
                    Run x = r.getFulfillment().clone();
                    addRequirements(x, linkMetaMap, previous);
                    run.splice(x.root());
                });
            }
            previous.add(chain);
        }
        run.getNextRuns().stream().collect(Collectors.toList()).forEach(r->addRequirements(r, linkMetaMap, new HashSet<>(){{addAll(previous);}}));
    }
    public void addRequirements(NucleoMetaHandler linkMetaMap){
        addRequirements(start, linkMetaMap, new HashSet<>());
    }
    public void optimize(NucleoMetaHandler linkMetaMap){
        removeDuplicates(start, new HashSet<>(), linkMetaMap);
    }
    public void optimize(){
        removeDuplicates(start, new HashSet<>());
    }
    public void buildGraph(Graph<VertexChain, DefaultEdge> graph, VertexChain previous, Run run, Map<Object, VertexChain> others){
        if(run.getClass() == SingularRun.class || run.getClass() == BroadcastRun.class){
            String chain = ((run.getClass() == BroadcastRun.class)?"Broadcast: ":"")+((SingularRun) run).getChain();
            VertexChain  currentChain;
            if(others.containsKey(run)){
                currentChain = others.get(run);
            }else{
                currentChain = new VertexChain(chain);
                graph.addVertex(currentChain);
                others.put(run, currentChain);
            }
            if (previous != null) {
                graph.addEdge(previous, currentChain, new SimpleEdge());
            }
            run.getNextRuns().forEach(r->buildGraph(graph, currentChain, r,others));
        }else if(run.getClass() == ParallelRun.class){
            run.getNextRuns().forEach(r->buildGraph(graph, previous, r, others));
        }
    }

    public Graph<VertexChain, DefaultEdge> display(){
        Graph<VertexChain, DefaultEdge> graph = new DefaultDirectedGraph<>(DefaultEdge.class);
        VertexChain vertexChain = new VertexChain("nucleo.client.root");
        graph.addVertex(vertexChain);
        buildGraph(graph, vertexChain, start, new HashMap<>());
        return graph;
    }

    public static PathPlusLeafs generateSerialRun(Object... chains) throws PathGenerationException {
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
                } else if (i!=0){
                    throw new PathGenerationException("Unable to complete chain, no previous chain to link to.");
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
                    } else if (i!=0){
                        throw new PathGenerationException("Unable to complete chain, no previous chain to link to.");
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
    public static PathPlusLeafs generateRun(String chain) throws PathGenerationException {
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

    public static PathPlusLeafs generateBroadcastRun(String broadcastChain, String... chains) throws PathGenerationException {
        PathPlusLeafs pathPlusLeafs = new PathPlusLeafs();
        if(chains != null){
            PathPlusLeafs fullChain = generateSerialRun(chains);
            pathPlusLeafs.setRoot(fullChain.getRoot());
        }
        BroadcastRun broadcastRun = new BroadcastRun(broadcastChain);
        if(pathPlusLeafs.getRoot()==null){
            pathPlusLeafs.setRoot(broadcastRun);
        }else{
            pathPlusLeafs.getLeafs().forEach(r->r.getNextRuns().add(broadcastRun));
            broadcastRun.getParents().addAll(pathPlusLeafs.getLeafs());
        }
        pathPlusLeafs.setLeafs(new LinkedList<>());
        return pathPlusLeafs;
    }
    public static PathPlusLeafs generateBroadcastRun(String broadCastchain) throws PathGenerationException {
        return generateBroadcastRun(broadCastchain, null);
    }


    public PathBuilder addParallel(PathPlusLeafs... pathPlusLeafsOld) throws PathGenerationException {
        PathPlusLeafs pathPlusLeafs = generateParallelRun(pathPlusLeafsOld);
        Run.traverseModify(pathPlusLeafs.getRoot(), (run)->{
           run.setParallel(true);
        });
        if(currentLeafs.size()>0){
            currentLeafs.forEach(leaf->{
                pathPlusLeafs.getRoot().getParents().addAll(currentLeafs);
                leaf.getNextRuns().add(pathPlusLeafs.getRoot());
            });
        } else if(start !=null){
            throw new PathGenerationException("Unable to link chains, no path to previous chains.");
        }
        currentLeafs = pathPlusLeafs.getLeafs();

        if(start==null)
            start = pathPlusLeafs.getRoot();

        return this;
    }

    public PathBuilder add(Object... chains) throws PathGenerationException {
        PathPlusLeafs pathPlusLeafs = generateSerialRun(chains);
        if (currentLeafs.size() > 0) {
            currentLeafs.forEach(leaf ->{
                pathPlusLeafs.getRoot().getParents().addAll(currentLeafs);
                leaf.getNextRuns().add(pathPlusLeafs.getRoot());
            });
        } else if(start != null){
            throw new PathGenerationException("Unable to link chains, no path to previous chains.");
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

    //String possibleString = "{[information.hits,information.test],popcorn,{[information.hits,information.test],information.test}},information.test,information.popcorn,{[information.hits,information.test],popcorn}";
    public static void main(String[] args) throws PathGenerationException {
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
