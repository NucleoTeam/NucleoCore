package com.synload.nucleo.elastic;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.synload.nucleo.event.NucleoData;
import org.apache.http.HttpHost;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;

import java.util.ArrayList;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;

public class ElasticSearchPusher implements Runnable {
  private RestHighLevelClient client;
  private ObjectMapper om = new ObjectMapper();
  private List<NucleoData> queue = new ArrayList<>();
  public ElasticSearchPusher(String server, int port, String scheme){
    client = new RestHighLevelClient(
      RestClient.builder(
        new HttpHost(server, port, scheme)
      )
    );

  }
  public void add(NucleoData item) {
    try {
      if(item.getTrack()==1) {
        item.setVersion(item.getVersion() + 1);
        NucleoData itemTemp = (NucleoData) item.clone();
        queue.add(itemTemp);
      }
    }catch (Exception e){
      e.printStackTrace();
    }
  }
  public void run(){
    while(true){
      try {
        if (queue.size() > 0) {
          NucleoData data = queue.remove(0);
          byte[] object = om.writeValueAsBytes(data);
          IndexRequest request = new IndexRequest()
              .index("nucleo")
              .id(data.getOrigin() + "-" + data.getRoot().toString())
              .version(data.getVersion())
              .versionType(VersionType.EXTERNAL)
              .source(object, XContentType.JSON);
          client.index(request, RequestOptions.DEFAULT);
        }
      }catch (ElasticsearchStatusException x){
        //x.printStackTrace();
      }catch (Exception e){
        //e.printStackTrace();
      }
      try{
        Thread.sleep(1);
      } catch (Exception e){ }
    }
  }
}
