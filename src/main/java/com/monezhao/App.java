package com.monezhao;


import io.vertx.core.json.JsonObject;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.logging.log4j.Log4jESLoggerFactory;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.FilterBuilders;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Objects;

/**
 * Hello world!
 */
public class App {

//  public static final String index = "dashboard";
    public static final String index = "megacorp";
//  public static final String type = "dashboard";
  public static final String type = "employee";

  public static void main(String[] args) throws IOException {
    Log4jESLoggerFactory.getRootLogger().setLevel("ERROR");
    Settings settings = ImmutableSettings.settingsBuilder().put("cluster.name", "ciphergateway").build();
    Client client = new TransportClient(settings)
        .addTransportAddress(new InetSocketTransportAddress("localhost", 9300));

    exportES(client);
//    deleteES(client);
//    importES(client);
    client.close();
  }

  private static void importES(Client client) throws IOException {
    String filePath = "es.txt";
    File file = new File(filePath);
    if (!file.exists()) {
      System.out.println("文件不存在");
      return;
    }
    try (BufferedReader br = new BufferedReader(new FileReader(filePath))) {
      //读取刚才导出的ES数据

      String string;
      int count = 0;
      //开启批量插入
      BulkRequestBuilder bulkRequest = client.prepareBulk();
      while ((string = br.readLine()) != null) {
        if (Objects.equals(string, "")) continue;
        JsonObject jsonObject = new JsonObject(string);
        System.out.println(jsonObject);
        String ESindex;
        if (jsonObject.containsKey("ESindex")) {
          ESindex = (String) jsonObject.remove("ESindex");
        } else {
          ESindex = index;
        }
        String EStype;
        if (jsonObject.containsKey("EStype")) {
          EStype = (String) jsonObject.remove("EStype");
        } else {
          EStype = type;
        }
        ++count;
        bulkRequest.add(client.prepareIndex(ESindex, EStype, String.valueOf(count)).setSource(jsonObject.toString()));
        //每一千条提交一次
        if (count % 1000 == 0) {
          bulkRequest.execute().actionGet();
          System.out.println("提交了：" + count);
        }
      }
      bulkRequest.execute().actionGet();
      System.out.println("插入完毕");
    }
  }

  private static void deleteES(Client client) {
    BulkRequestBuilder bulkRequest = client.prepareBulk();
    SearchResponse response = client.prepareSearch(index).setTypes(type)
        .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
        .setQuery(QueryBuilders.matchAllQuery())
        .setFrom(0).setSize(20).setExplain(true).execute().actionGet();
    System.out.println("length: " + response.getHits().getHits().length);
    if (response.getHits().getHits().length != 0) {
      for (SearchHit hit : response.getHits()) {
        String id = hit.getId();
        System.out.println("id: " + id);
        bulkRequest.add(client.prepareDelete(index, type, id).request());
      }
      BulkResponse bulkResponse = bulkRequest.get();
      if (bulkResponse.hasFailures()) {
        for (BulkItemResponse item : bulkResponse.getItems()) {
          System.out.println(item.getFailureMessage());
        }
      } else {
        System.out.println("delete ok");
      }
    } else {
      System.out.println("delete ok");
    }
  }

  private static void exportES(Client client) throws IOException {
    SearchResponse response = client.prepareSearch(index).setTypes(type)
        .setQuery(
            QueryBuilders.filteredQuery(
                QueryBuilders.matchPhraseQuery("operation", "账号密码登录"),
                FilterBuilders.rangeFilter("opertime").from(1494916514776L).to(1494916556062L)
                ))
//        .addSort(SortBuilders.fieldSort("opertime"))
        .setSize(10000)
        .setScroll(new TimeValue(600000))
        .setSearchType(SearchType.SCAN).execute().actionGet();//setSearchType(SearchType.Scan) 告诉ES不需要排序只要结果返回即可 setScroll(new TimeValue(600000)) 设置滚动的时间
    String scrollid = response.getScrollId();

    String filePath = "es.txt";
    File file = new File(filePath);
    System.out.println(file.getAbsolutePath());
    System.out.println("delete file " + file.delete());
    System.out.println("create file " + file.createNewFile());
    try (BufferedWriter out = new BufferedWriter(new FileWriter(filePath, true))) {
      //把导出的结果以JSON的格式写到文件里,每次返回数据10000条。一直循环查询直到所有的数据都查询出来
      while (true) {
        SearchResponse response2 = client.prepareSearchScroll(scrollid).setScroll(new TimeValue(1000000))
            .execute().actionGet();
        SearchHits searchHit = response2.getHits();
        //再次查询不到数据时跳出循环
        if (searchHit.getHits().length == 0) {
          break;
        }
        for (int i = 0; i < searchHit.getHits().length; i++) {
          SearchHit hit = searchHit.getHits()[i];
          System.out.println(hit.getSourceAsString());
          out.write(
              new JsonObject(hit.getSourceAsString())
                  .put("ESindex", hit.index())
                  .put("EStype", hit.type())
                  .toString()
          );
          out.write("\n");
        }
      }
    }
  }
}
