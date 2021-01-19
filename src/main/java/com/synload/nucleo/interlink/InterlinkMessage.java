package com.synload.nucleo.interlink;

import com.synload.nucleo.data.NucleoData;
import com.synload.nucleo.zookeeper.ServiceInformation;

import java.io.Serializable;

public class InterlinkMessage implements Serializable {
  public InterlinkMessage(){}
  public NucleoData data = null;
  public ServiceInformation information = null;
  public String topic = null;
  public InterlinkMessage(String topic, NucleoData data){
    this.topic = topic;
    this.data = data;
  }
  public InterlinkMessage(String topic, ServiceInformation information){
    this.topic = topic;
    this.information = information;
  }
  public NucleoData getData() {
    return data;
  }

  public void setData(NucleoData data) {
    this.data = data;
  }

  public String getTopic() {
    return topic;
  }

  public void setTopic(String topic) {
    this.topic = topic;
  }

  public ServiceInformation getInformation() {
    return information;
  }

  public void setInformation(ServiceInformation information) {
    this.information = information;
  }
}