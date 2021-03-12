package com.synload.nucleo.chain;

import com.synload.nucleo.chain.path.ParallelRun;
import com.synload.nucleo.chain.path.Run;
import com.synload.nucleo.chain.path.SingularRun;
import org.apache.commons.lang3.SerializationException;

import java.io.*;
import java.util.LinkedList;
import java.util.List;
import java.util.Stack;
import java.util.stream.Collectors;

public class ChainExecution implements Serializable {
    Run current = null;
    Run root = null;
    List<String> history = new LinkedList<>();

    public ChainExecution(Run start) {
        this.current = start;
        this.root = start;
    }

    public List<ChainExecution> next(){
        List<ChainExecution> nexts = new LinkedList<>();
        this.current.getNextRuns().stream().forEach(r->{
            if(r.getClass() == ParallelRun.class){
                r.getNextRuns().forEach(ru->{
                    ChainExecution chainExecution = this.clone();
                    chainExecution.getHistory().add(((SingularRun)this.getCurrent()).getChain());
                    chainExecution.setCurrent(ru);
                    nexts.add(chainExecution);
                });
            }else{
                ChainExecution chainExecution = this.clone();
                chainExecution.getHistory().add(((SingularRun)this.getCurrent()).getChain());
                chainExecution.setCurrent(r);
                nexts.add(chainExecution);
            }
        });
        return nexts;
    }

    public Run getCurrent() {
        return current;
    }

    public void setCurrent(Run current) {
        this.current = current;
    }

    public Run getRoot() {
        return root;
    }

    public void setRoot(Run root) {
        this.root = root;
    }

    public List<String> getHistory() {
        return history;
    }

    public void setHistory(List<String> history) {
        this.history = history;
    }

    protected ChainExecution clone() {
        ChainExecution execution = null;
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
            execution = (ChainExecution) in.readObject();
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
        return execution;
    }
}
