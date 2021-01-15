package com.synload.nucleo.utils;

import com.google.common.collect.Lists;
import com.synload.nucleo.data.NucleoData;
import com.synload.nucleo.event.NucleoStep;

import java.util.List;
import java.util.stream.Collectors;

public class NucleoDataStats {
    NucleoStep slowest;
    NucleoStep fastest;
    Long slowestDuration;
    Long fastestDuration;
    long median;
    float average;
    public NucleoDataStats(){}
    public void calculate(NucleoData nucleoData){
        List<Long> listOfDurations = nucleoData.getSteps().stream().map(step->{
            slowest(step);
            fastest(step);
            average += step.getTotal();
            return step.getTotal();
        }).sorted(Long::compareTo).collect(Collectors.toList());
        median = listOfDurations.get(Math.round(listOfDurations.size()/2));
        average = average / listOfDurations.size();
    }
    public void slowest(NucleoStep step){
        long duration = step.getEnd()-step.getStart();
        if(slowestDuration!=null){
            if(slowestDuration.longValue()<duration){
                slowestDuration = duration;
                slowest = step;
            }
        }else{
            slowestDuration = duration;
            slowest = step;
        }
    }
    public void fastest(NucleoStep step){
        long duration = step.getEnd()-step.getStart();
        if(fastestDuration!=null){
            if(fastestDuration.longValue()>duration){
                fastestDuration = duration;
                fastest = step;
            }
        }else{
            fastestDuration = duration;
            fastest = step;
        }
    }

    public NucleoStep getSlowest() {
        return slowest;
    }

    public NucleoStep getFastest() {
        return fastest;
    }

    public Long getSlowestDuration() {
        return slowestDuration;
    }

    public Long getFastestDuration() {
        return fastestDuration;
    }

    public long getMedian() {
        return median;
    }

    public float getAverage() {
        return average;
    }
}
