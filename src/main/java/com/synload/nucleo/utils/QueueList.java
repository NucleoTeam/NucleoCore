package com.synload.nucleo.utils;

public class QueueList{
    private Node last=null;
    private Node first=null;
    private int size = 0;
    private Action a;
    private Object read=null;
    private class Node{
        public Node next = null;
        public Object o;
        public Node(Object o){
            this.o=o;
        }
    }
    private class Action implements Runnable{
        private QueueList queue;
        private Object[] objectWriteQueue = new Object[50];
        private int i = 0;
        private int l = 0;
        private int j = 0;
        public Action(QueueList queue){
            this.queue = queue;
        }
        public void run(){
            while(true){
                //System.out.println("b: i: "+i+" j: "+j);
                if(i!=j){
                    add();
                }
                //System.out.println("a: i: "+i+" j: "+j);
                if(queue.read==null) {
                    removeFirst();
                }
                try{
                    Thread.sleep(1);
                }catch(InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
        public void add(Object o){

            i++;
            if(i>=50){
                i=0;
            }
            objectWriteQueue[i]=o;
            //System.out.println("i: "+i+" j: "+j);

        }
        public void removeFirst(){
            Node n = queue.getFirst();
            if(n!=null) {
                queue.setFirst(n.next);
                if (queue.getFirst() == null) {
                    queue.setLast(null);
                }
                queue.setRead(n.o);
            }
        }
        public void add(){
            int k=j;
            while( k>i || k<i ) {
                k++;
                if(k>=50){
                    k=0;
                }
                if(objectWriteQueue[k]!=null) {
                    Node n = new Node(objectWriteQueue[k]);
                    objectWriteQueue[k]=null;
                    n.next = null;
                    if(queue.getLast()!=null) {
                        queue.getLast().next = n;
                    }
                    queue.setLast(n);
                    if (queue.getFirst() == null) {
                        queue.setFirst(n);
                    }
                }
            }
            j=k;
        }
    }
    public QueueList(){
        a = new Action(this);
        new Thread(a).start();
    }
    public void add(Object o){
        a.add(o);
    }

    public boolean hasNext(){
        return read!=null;
    }

    public Object getRead() {
        Object o = read;
        read = null;
        return o;
    }

    public void setRead(Object read) {
        this.read = read;
    }

    public Node getLast() {
        return last;
    }

    public void setLast(Node last) {
        this.last = last;
    }

    public Node getFirst() {
        return first;
    }

    public void setFirst(Node first) {
        this.first = first;
    }

    public int getSize() {
        return size;
    }

    public void setSize(int size) {
        this.size = size;
    }
}
