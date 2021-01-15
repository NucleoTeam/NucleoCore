package com.synload.nucleo.data;

import com.google.common.collect.Lists;

import java.util.ArrayList;
import java.util.List;

public class NucleoObjectPath {
    public static class Option {
        public enum Type{
            EQUAL,
            NOT_EQUAL,
            GREATER,
            LESSER,
            IN
        }
        List<String> path = Lists.newLinkedList();
        Type operator;
        String value;
        Character combiner = null;
        public Option() {
        }
        public List<String> getPath() {
            return path;
        }
        public void setPath(List<String> path) {
            this.path = path;
        }
        public Type getOperator() {
            return operator;
        }
        public void setOperator(Type operator) {
            this.operator = operator;
        }
        public String getValue() {
            return value;
        }
        public void setValue(String value) {
            this.value = value;
        }
        public Character getCombiner() {
            return combiner;
        }
        public void setCombiner(Character combiner) {
            this.combiner = combiner;
        }
    }
    public static class Path {
        String path;
        List<Option> options = Lists.newLinkedList();
        public Path(String path) {
            this.path = path;
        }
        public String getPath() {
            return path;
        }
        public void setPath(String path) {
            this.path = path;
        }
        public List<Option> getOptions() {
            return options;
        }
        public void setOptions(List<Option> options) {
            this.options = options;
        }
    }
    public static Path[] pathArray(String pathString) {
        List<Path> paths = new ArrayList<>();
        int start = 0;
        int inside = 0;
        int len = pathString.length();
        Path latest = null;

        Option latestOption = null;
        for (int x = 0; x < len; x++) {
            char characterMatch = pathString.charAt(x);
            switch (characterMatch) {
                case '.':
                    if (inside == 0) {
                        if(latest!=null) {
                            paths.add(latest);
                            latest=null;
                            start = x + 1;
                        }else{
                            int finalX = x;
                            int finalStart = start;
                            paths.add(new Path(pathString.substring(finalStart, finalX)));
                            start = x + 1;
                        }
                    }else if(inside==1){
                        latestOption.getPath().add(pathString.substring(start, x));
                        start = x + 1;
                    }else{
                        return null;
                    }
                    break;
                case '=':
                    latestOption.getPath().add(pathString.substring(start, x));
                    latestOption.setOperator(Option.Type.EQUAL);
                    start = x + 1;
                    break;
                case '\'':
                    latestOption.getPath().add(pathString.substring(start, x));
                    latestOption.setOperator(Option.Type.NOT_EQUAL);
                    start = x + 1;
                    break;
                case '^':
                    latestOption.getPath().add(pathString.substring(start, x));
                    latestOption.setOperator(Option.Type.IN);
                    start = x + 1;
                    break;
                case '>':
                    latestOption.getPath().add(pathString.substring(start, x));
                    latestOption.setOperator(Option.Type.GREATER);
                    start = x + 1;
                    break;
                case '<':
                    latestOption.getPath().add(pathString.substring(start, x));
                    latestOption.setOperator(Option.Type.LESSER);
                    start = x + 1;
                    break;
                case '&':
                    latestOption.setValue(pathString.substring(start, x));
                    latest.getOptions().add(latestOption);
                    latestOption = new Option();
                    latestOption.setCombiner('&');
                    start = x + 1;
                    break;
                case '|':
                    latestOption.setValue(pathString.substring(start, x));
                    latest.getOptions().add(latestOption);
                    latestOption = new Option();
                    latestOption.setCombiner('|');
                    start = x + 1;
                    break;
                case '[':
                    int finalX1 = x;
                    int finalStart1 = start;
                    latest = new Path(pathString.substring(finalStart1, finalX1));
                    start = x + 1;
                    latestOption = new Option();
                    inside += 1;
                    break;
                case ']':
                    latestOption.setValue(pathString.substring(start, x));
                    latest.getOptions().add(latestOption);
                    latestOption = null;
                    start = x;
                    inside -= 1;
                    break;
            }
        }
        if(start<len){
            if(latest!=null){
                paths.add(latest);
            }else{
                paths.add(new Path(pathString.substring(start, len)));
            }

        }
        final int[] idx = {0};
        Path[] pathArray = new Path[paths.size()];
        paths.forEach(path->{
            pathArray[idx[0]] = path;
            idx[0]++;
        });
        return pathArray;
    }
}
