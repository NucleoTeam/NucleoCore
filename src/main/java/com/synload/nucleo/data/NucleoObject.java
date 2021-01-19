package com.synload.nucleo.data;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Queues;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.beans.IntrospectionException;
import java.beans.Introspector;
import java.beans.PropertyDescriptor;
import java.io.*;
import java.lang.reflect.InvocationTargetException;
import java.util.*;
import java.util.function.Predicate;
import java.util.stream.Collectors;

public class NucleoObject implements Serializable {

    private List<NucleoChange> changes = Lists.newLinkedList();
    private Map<String, Object> objects = Maps.newHashMap();

    @JsonIgnore
    private Map<String, Object> currentObjects;

    private boolean ledgerMode = false;
    private int step;

    @JsonIgnore
    private ObjectMapper mapper = new ObjectMapper(){{
        this.enableDefaultTyping();
        this.setSerializationInclusion(JsonInclude.Include.NON_NULL);
    }};

    @JsonIgnore protected static final Logger logger = LoggerFactory.getLogger(NucleoObject.class);

    private Map<String, Object> getWorkingObjects(){
        if(currentObjects==null){
            currentObjects = objects;
        }
        return currentObjects;
    }

    public NucleoObject() {

    }



    public NucleoObject(NucleoObject old) {
        if(old.objects!=null){
            objects = (new HashMap<>());
            objects.putAll(old.objects);
        }else{
            objects = Maps.newTreeMap();
        }
        if(old.changes!=null){
            changes = Lists.newLinkedList(old.changes);
        }else{
            changes = Lists.newLinkedList();
        }
    }

    Object fullCopy(Object objectToCopy){
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        ObjectOutputStream objectOutputStream = null;
        ByteArrayInputStream bis = null;
        Object copiedObject=null;
        try {
            objectOutputStream = new ObjectOutputStream(byteArrayOutputStream);
            objectOutputStream.writeObject(objectToCopy);
            objectOutputStream.flush();
            bis = new ByteArrayInputStream(byteArrayOutputStream.toByteArray());
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        } finally {
            try {
                if (byteArrayOutputStream != null)
                    byteArrayOutputStream.close();
            }catch (Exception e){
                e.printStackTrace();
            }
            try {
                if (objectOutputStream != null)
                    objectOutputStream.close();
            }catch (Exception e){
                e.printStackTrace();
            }
        }
        if(bis!=null) {
            ObjectInput in = null;
            try {
                in = new ObjectInputStream(bis);
                copiedObject = in.readObject();
            } catch (IOException | ClassNotFoundException e) {
                e.printStackTrace();
                return null;
            } finally {
                try {
                    if (in != null) {
                        in.close();
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
                try {
                    bis.close();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
        return objectToCopy;
    }
    public void mergeChanges() {  // do not write changes to objects, just add add/delete/update
        Map<String, Object> tmp = (Map<String, Object>)this.fullCopy(getWorkingObjects());
        Lists.newLinkedList(this.changes).stream().sorted(Comparator.comparingInt(NucleoChange::getParallelStep).thenComparingInt(a -> a.getPriority().value)).forEach(c->{
            if(c.getType()==NucleoChangeType.CREATE)
                createWrite(c.getObjectPath(), c.storedObject());
            if(c.getType()==NucleoChangeType.UPDATE)
                update(c.getObjectPath(), getObjects());
            if(c.getType()==NucleoChangeType.DELETE)
                delete(c.getObjectPath());
        });
        this.changes.clear();
        currentObjects = tmp;
    }

    public Map<String, Object> getObjects() {
        return objects;
    }

    public void setObjects(Map<String, Object> objects) {
        this.objects = objects;
    }

    public Queue splitPath(String objectPath){
        return Queues.newArrayDeque(Arrays.asList(objectPath.split("\\.")));
    }

    public Object getValueFromObject(String objectPath, Object object){
        NucleoObjectPath.Path[] paths = NucleoObjectPath.pathArray(objectPath);
        if(paths.length==0){
            return null;
        }
        int x=-1;
        int lenPaths = paths.length;
        for(x=0;x<lenPaths;x++){
            if(object==null)
                return null;
            NucleoObjectPath.Path path = paths[x];
            if(object instanceof Map){
                if(((Map<String, Object>)object).containsKey(path.getPath())){
                    object = ((Map<String, Object>)object).get(path.getPath());
                }else{
                    return null;
                }
            }else if(object instanceof List){
                Predicate predicates = null;
                int len = path.getOptions().size();
                for (int i = 0; i < len; i++) {
                    NucleoObjectPath.Option option = path.getOptions().get(i);
                    if(option.operator!=null) {
                        if (predicates == null) {
                            predicates = createPredicate(option);
                        } else {
                            Predicate predicate = createPredicate(option);
                            if(option.getCombiner().equals('&')){
                                predicates = predicate.and(predicates);
                            }else if(option.getCombiner().equals('|')){
                                predicates = predicate.or(predicates);
                            }
                        }
                    }else{
                        if(len>1){
                            return null;
                        }
                        int idx = Integer.valueOf(option.getValue()).intValue();
                        if(idx<((List) object).size() && idx>=0) {
                            object = ((List) object).get(idx);
                        }else{
                            return null;
                        }
                        break;
                    }
                }
                if(predicates!=null) {
                    List obj = (List) ((List) object).stream().filter(predicates).collect(Collectors.toList());
                    if (obj.size() > 0) {
                        object = obj.get(0);
                    }else{
                        return null;
                    }
                }else{
                    return null;
                }
            }else if(object instanceof Object){
                try {
                    PropertyDescriptor pd = Arrays.stream(Introspector.getBeanInfo(object.getClass()).getPropertyDescriptors()).filter(f->f.getName().equals(path.getPath())).findFirst().get();
                    object = pd.getReadMethod().invoke(object);
                } catch (IllegalAccessException | IntrospectionException | InvocationTargetException e) {
                    e.printStackTrace();
                    return null;
                }
            }else{
                return null;
            }
            if(lenPaths-1==x) {
                return object;
            }
        }
        return object;
    }

    public Object get(String objectPath){
        return getValueFromObject(objectPath, getWorkingObjects());
    }

    public void createOrUpdate(String objectPath, Object object, NucleoChangePriority priority){
        if(get(objectPath)!=null){
            update(objectPath, object, priority);
        }
        create(objectPath, object, priority);
    }

    public void createOrUpdate(String objectPath, Object object){
        if(get(objectPath)!=null){
            update(objectPath, object);
        }
        create(objectPath, object);
    }

    private boolean createWrite(String objectPath, Object object){
        Map<String, Object> tmp = getWorkingObjects();
        NucleoObjectPath.Path[] entries = NucleoObjectPath.pathArray(objectPath);
        if(entries.length==0){
            return false;
        }
        int x=0;
        if(entries.length>1) {
            for (x = 0; x < entries.length - 1; x++) {
                if (!tmp.containsKey(entries[x].getPath())) {
                    tmp.put(entries[x].getPath(), Maps.newTreeMap());
                }
                tmp = (Map<String, Object>) tmp.get(entries[x].getPath());
            }
        }
        if(!tmp.containsKey(entries[x].getPath())) {
            tmp.put(entries[x].getPath(), object);
            return true;
        }
        return false;
    }

    public boolean create(String objectPath, Object object, NucleoChangePriority priority){
        boolean success = createWrite(objectPath, object);
        if(isLedgerMode() && success){
            changes.add(new NucleoChange(NucleoChangeType.CREATE, objectPath, object, step, priority));
        }
        return success;
    }
    public boolean create(String objectPath, Object object){
        return create(objectPath, object, NucleoChangePriority.NONE);
    }


    private boolean updateWrite(String objectPath, Object object){
        Object tmp = getWorkingObjects();
        NucleoObjectPath.Path[] paths = NucleoObjectPath.pathArray(objectPath);
        if(paths.length==0){
            return false;
        }
        int x=-1;
        int lenPaths = paths.length;
        for(x=0;x<lenPaths;x++){
            if(tmp==null)
                return false;
            NucleoObjectPath.Path path = paths[x];
            if(tmp instanceof Map){
                if(((Map<String, Object>)tmp).containsKey(path.getPath())){
                    if(lenPaths-1==x) {
                        ((Map<String, Object>)tmp).put(path.getPath(), object);
                        return true;
                    }
                    tmp = ((Map<String, Object>)tmp).get(path.getPath());
                }
            }else if(tmp instanceof List){
                Predicate predicates = null;
                int len = path.getOptions().size();
                for (int i = 0; i < len; i++) {
                    NucleoObjectPath.Option option = path.getOptions().get(i);
                    if(option.operator!=null) {
                        if (predicates == null) {
                            predicates = createPredicate(option);
                        } else {
                            Predicate predicate = createPredicate(option);
                            if(option.getCombiner().equals('&')){
                                predicates = predicate.and(predicates);
                            }else if(option.getCombiner().equals('|')){
                                predicates = predicate.or(predicates);
                            }
                        }
                    }else{
                        if(len>1){
                            return false;
                        }
                        if(lenPaths-1==x) {
                            ((List) tmp).set(Integer.valueOf(option.getValue()).intValue(), object);
                            return true;
                        }
                        int idx = Integer.valueOf(option.getValue()).intValue();
                        if(idx<((List) tmp).size() && idx>=0) {
                            tmp = ((List) tmp).get(idx);
                        }else{
                            return false;
                        }
                        break;
                    }
                }
                if(predicates!=null) {
                    List obj = (List) ((List) tmp).stream().filter(predicates).collect(Collectors.toList());
                    if (obj.size()>0) {
                        if(lenPaths-1==x) {
                            ((List) tmp).set(((List) tmp).indexOf(obj.get(0)), object);
                            return true;
                        }
                        tmp = obj.get(0);
                    }else{
                        return false;
                    }
                }
            }else if(tmp instanceof Object){
                try {
                    if(lenPaths-1==x) {
                        PropertyDescriptor pd = Arrays.stream(Introspector.getBeanInfo(tmp.getClass()).getPropertyDescriptors()).filter(f->f.getName().equals(path.getPath())).findFirst().get();
                        pd.getWriteMethod().invoke(tmp, object);
                        return true;
                    }else {
                        PropertyDescriptor pd = Arrays.stream(Introspector.getBeanInfo(tmp.getClass()).getPropertyDescriptors()).filter(f->f.getName().equals(path.getPath())).findFirst().get();
                        tmp = pd.getReadMethod().invoke(tmp);
                    }
                } catch (IllegalAccessException | IntrospectionException | InvocationTargetException e) {
                    e.printStackTrace();
                    return false;
                }
            }
        }
        return false;
    }

    public boolean update(String objectPath, Object object, NucleoChangePriority priority){
        if(get(objectPath)!=null){
            boolean success = updateWrite(objectPath, object);
            if(isLedgerMode() && success){
                changes.add(new NucleoChange(NucleoChangeType.UPDATE, objectPath, object, step, priority));
            }
            return success;
        }
        return false;
    }
    public boolean update(String objectPath, Object object){
        return update(objectPath, object, NucleoChangePriority.NONE);
    }

    private boolean deleteWrite(String objectPath){
        Object tmp = getWorkingObjects();
        NucleoObjectPath.Path[] paths = NucleoObjectPath.pathArray(objectPath);
        if(paths.length==0){
            return false;
        }
        int x=-1;
        int lenPaths = paths.length;
        for(x=0;x<lenPaths;x++){
            if(tmp==null)
                return false;
            NucleoObjectPath.Path path = paths[x];
            if(tmp instanceof Map){
                if(((Map<String, Object>)tmp).containsKey(path.getPath())){
                    if(lenPaths-1==x) {
                        ((Map<String, Object>)tmp).remove(path.getPath());
                        return true;
                    }
                    tmp = ((Map<String, Object>)tmp).get(path.getPath());
                }
            }else if(tmp instanceof List){
                Predicate predicates = null;
                int len = path.getOptions().size();
                for (int i = 0; i < len; i++) {
                    NucleoObjectPath.Option option = path.getOptions().get(i);
                    if(option.operator!=null) {
                        if (predicates == null) {
                            predicates = createPredicate(option);
                        } else {
                            Predicate predicate = createPredicate(option);
                            if(option.getCombiner().equals('&')){
                                predicates = predicate.and(predicates);
                            }else if(option.getCombiner().equals('|')){
                                predicates = predicate.or(predicates);
                            }
                        }
                    }else{
                        if(len>1){
                            return false;
                        }
                        if(lenPaths-1==x) {
                            int idx = Integer.valueOf(option.getValue()).intValue();
                            if(idx<((List) tmp).size()) {
                                ((List) tmp).remove(idx);
                                return true;
                            }
                            return false;
                        }
                        int idx = Integer.valueOf(option.getValue()).intValue();
                        if(idx<((List) tmp).size() && idx>=0) {
                            tmp = ((List) tmp).get(idx);
                        }else{
                            return false;
                        }
                        break;
                    }
                }
                if(predicates!=null) {
                    List obj = (List) ((List) tmp).stream().filter(predicates).collect(Collectors.toList());
                    if(lenPaths-1==x) {
                        ((List) tmp).removeIf(predicates);
                        return true;
                    }

                    if (obj.size() >0) {
                        tmp = obj.get(0);
                    }else{
                        return false;
                    }
                }
            }else if(tmp instanceof Object){
                try {
                    PropertyDescriptor pd = Arrays.stream(Introspector.getBeanInfo(tmp.getClass()).getPropertyDescriptors()).filter(f->f.getName().equals(path.getPath())).findFirst().get();
                    tmp = pd.getReadMethod().invoke(tmp);
                } catch (IllegalAccessException | IntrospectionException | InvocationTargetException e) {
                    e.printStackTrace();
                    return false;
                }
            }
        }
        return false;
    }

    public boolean delete(String objectPath, NucleoChangePriority priority){
        if(get(objectPath)!=null){
            boolean success = deleteWrite(objectPath);
            if(isLedgerMode() && success){
                changes.add(new NucleoChange(NucleoChangeType.DELETE, objectPath, null, step, priority));
            }
            return success;
        }
        return false;
    }
    public boolean delete(String objectPath){
        return delete(objectPath, NucleoChangePriority.NONE);
    }

    private boolean addToListWrite(String objectPath, Object object){
        Object tmp = getWorkingObjects();
        NucleoObjectPath.Path[] paths = NucleoObjectPath.pathArray(objectPath);
        if(paths.length==0){
            return false;
        }
        int x=-1;
        int lenPaths = paths.length;
        for(x=0;x<lenPaths;x++){
            if(tmp==null)
                return false;
            NucleoObjectPath.Path path = paths[x];
            if(tmp instanceof Map){
                if(((Map<String, Object>)tmp).containsKey(path.getPath())){
                    tmp = ((Map<String, Object>)tmp).get(path.getPath());
                }
            }else if(tmp instanceof List){
                Predicate predicates = null;
                int len = path.getOptions().size();
                for (int i = 0; i < len; i++) {
                    NucleoObjectPath.Option option = path.getOptions().get(i);
                    if(option.operator!=null) {
                        if (predicates == null) {
                            predicates = createPredicate(option);
                        } else {
                            Predicate predicate = createPredicate(option);
                            if(option.getCombiner().equals('&')){
                                predicates = predicate.and(predicates);
                            }else if(option.getCombiner().equals('|')){
                                predicates = predicate.or(predicates);
                            }
                        }
                    }else{
                        if(len>1){
                            return false;
                        }
                        int idx = Integer.valueOf(option.getValue()).intValue();
                        if(idx<((List) tmp).size() && idx>=0) {
                            tmp = ((List) tmp).get(idx);
                        }else{
                            return false;
                        }
                        break;
                    }
                }
                if(predicates!=null) {
                    List obj = (List) ((List) tmp).stream().filter(predicates).collect(Collectors.toList());
                    if (obj.size() > 0) {
                        tmp = obj.get(0);
                    }else{
                        return false;
                    }
                }
            }else if(tmp instanceof Object){
                try {
                    PropertyDescriptor pd = Arrays.stream(Introspector.getBeanInfo(tmp.getClass()).getPropertyDescriptors()).filter(f->f.getName().equals(path.getPath())).findFirst().get();
                    tmp = pd.getReadMethod().invoke(tmp);
                } catch (IllegalAccessException | IntrospectionException | InvocationTargetException e) {
                    e.printStackTrace();
                    return false;
                }
            }
        }
        if(lenPaths==x && tmp instanceof List) {
            ((List) tmp).add(object);
            return true;
        }
        return false;
    }

    public boolean addToList(String objectPath, Object object, NucleoChangePriority priority){
        if(get(objectPath)!=null){
            boolean success = addToListWrite(objectPath, object);
            if(isLedgerMode() && success){
                changes.add(new NucleoChange(NucleoChangeType.ADD, objectPath, object, step, priority));
            }
            return success;
        }
        return false;
    }
    public boolean addToList(String objectPath, Object object){
        return addToList(objectPath, object, NucleoChangePriority.NONE);
    }

    public Predicate createPredicate(NucleoObjectPath.Option option){
        return (Predicate)(c)->{
            Object obj = c;
            String path = String.join(".", option.getPath());
            if(!path.equals("")) {
                obj = getValueFromObject(path, c);
            }
            if(obj==null){
                return false;
            }
            switch(option.getOperator()){
                case EQUAL:
                    return equal(obj, option.getValue());
                case NOT_EQUAL:
                    return notEqual(obj, option.getValue());
                case LESSER:
                    return lesser(obj, option.getValue());
                case GREATER:
                    return greater(obj, option.getValue());
                case IN:
                    return inList(obj, option.getValue());
                default:
                    System.exit(-1);
            }
            return false;
        };
    }
    public boolean equal(Object object, String value){
        if(object instanceof Integer){
            return object.equals(Integer.valueOf(value));
        }else if(object instanceof String){
            return object.equals(value);
        }else if(object instanceof Float){
            return object.equals(Float.valueOf(value));
        }else if(object instanceof Boolean){
            return object.equals(Boolean.valueOf(value));
        }else if(object instanceof Character){
            if(value.toCharArray().length>0) {
                return object.equals(Character.valueOf(value.toCharArray()[0]));
            }
        }
        return false;
    }
    public boolean greater(Object object, String value){
        if(object instanceof Integer){
            return ((Integer) object).compareTo(Integer.valueOf(value))>0;
        }else if(object instanceof String){
            return ((String) object).compareTo(value)>0;
        }else if(object instanceof Float){
            return ((Float) object).compareTo(Float.valueOf(value))>0;
        }else if(object instanceof Character){
            if(value.toCharArray().length>0) {
                return ((Character) object).compareTo(Character.valueOf(value.toCharArray()[0]))>0;
            }
        }
        return false;
    }
    public boolean lesser(Object object, String value){
        if(object instanceof Integer){
            return ((Integer) object).compareTo(Integer.valueOf(value))<0;
        }else if(object instanceof String){
            return ((String) object).compareTo(value)<0;
        }else if(object instanceof Float){
            return ((Float) object).compareTo(Float.valueOf(value))<0;
        }else if(object instanceof Character){
            if(value.toCharArray().length>0) {
                return ((Character) object).compareTo(Character.valueOf(value.toCharArray()[0]))<0;
            }
        }
        return false;
    }
    public boolean notEqual(Object object, String value){
        return !equal(object, value);
    }

    public boolean inList(Object object, String value){
        return Arrays.stream(value.substring(1).split(value.substring(0, 1))).map(compareVal->equal(object, compareVal)).filter(result->result).count()>0;
    }

    public int getStep() {
        return step;
    }

    public void setStep(int step) {
        this.step = step;
    }

    public boolean isLedgerMode() {
        return ledgerMode;
    }

    public void setLedgerMode(boolean ledgerMode) {
        this.ledgerMode = ledgerMode;
    }

    public boolean exists(String objectPath){
        return get(objectPath) != null;
    }

    public List<NucleoChange> getChanges() {
        return changes;
    }

    public void setChanges(List<NucleoChange> changes) {
        this.changes = changes;
    }

    public Map<String, Object> getCurrentObjects() {
        return currentObjects;
    }

    public void setCurrentObjects(Map<String, Object> currentObjects) {
        this.currentObjects = currentObjects;
    }
}
