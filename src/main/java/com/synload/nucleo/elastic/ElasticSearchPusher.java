package com.synload.nucleo.elastic;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.synload.nucleo.event.NucleoData;
import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.VersionType;

import java.util.Date;
import java.util.LinkedList;

public class ElasticSearchPusher implements Runnable {
  private RestHighLevelClient client;
  private ObjectMapper om = new ObjectMapper();
  private LinkedList<NucleoData> queue = new LinkedList<>();
  public ElasticSearchPusher(String server, int port, String scheme){
    client = new RestHighLevelClient(
      RestClient.builder(
        new HttpHost(server, port, scheme)
      )
    );

  }
  public void add(NucleoData item) {
    queue.add(item);
  }
  public void run(){
    while(true){
      try{
        if (queue.size() > 0) {
          NucleoData data = queue.pop();
          byte[] object = om.writeValueAsBytes(data);
          IndexRequest request = new IndexRequest("nucleo")
            .id(data.getOrigin()+"-"+data.getRoot().toString())
            .source(object, XContentType.JSON)
            .version(data.getSteps().size())
            .versionType(VersionType.EXTERNAL);
          IndexResponse indexResponse = client.index(request, RequestOptions.DEFAULT);
          System.out.println(indexResponse.toString());
        }
        Thread.sleep(1);
      }catch (Exception e){
        e.printStackTrace();
        System.exit(-1);
        return;
      }
    }
  }
}
