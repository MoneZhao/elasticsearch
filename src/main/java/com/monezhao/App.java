package com.monezhao;


import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.logging.log4j.Log4jESLoggerFactory;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.sort.SortOrder;

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

  public static final String index = "dashboard";
  //  public static final String index = "megacorp";
  public static final String type = "dashboard";
//  public static final String type = "employee";

  private static final int size = 10000;
  private static final int scroll = 600000;

  public static void main(String[] args) throws IOException {
    Log4jESLoggerFactory.getRootLogger().setLevel("ERROR");
    Settings settings = ImmutableSettings.settingsBuilder().put("cluster.name", "ciphergateway").build();
    //Use one and only one Client in your JVM. It's threadsafe.
    Client client = new TransportClient(settings)
        .addTransportAddress(new InetSocketTransportAddress("localhost", 9300));

    Long start = System.currentTimeMillis();
//    exportES(client);
//    deleteES(client);
//    importES(client);
//    searchES(client);
    exportAndDeleteES(client);
    client.close();
    Long end = System.currentTimeMillis();
    System.out.println("用时: " + (end - start) + " ms");
  }

  private static void exportAndDeleteES(Client client) throws IOException {
    SearchResponse response = client.prepareSearch(index).setTypes(type)
        .setQuery(QueryBuilders.matchAllQuery())
        .setSize(size)//max of hits will be returned for each scroll
        .setScroll(new TimeValue(scroll))//设置滚动的时间
        .setSearchType(SearchType.SCAN)//告诉ES不需要排序只要结果返回即可
        .get();
    BulkRequestBuilder bulkRequest = client.prepareBulk();

    String filePath = "es.txt";
    File file = new File(filePath);
    System.out.println(file.getAbsolutePath());
    System.out.println("delete file " + file.delete());
    System.out.println("create file " + file.createNewFile());

    try (BufferedWriter out = new BufferedWriter(new FileWriter(filePath, true))) {
      //把导出的结果以JSON的格式写到文件里,每次返回数据10000条。一直循环查询直到所有的数据都查询出来
      do {
        JsonArray idArray = new JsonArray();
        for (SearchHit hit : response.getHits().getHits()) {
          String id = hit.getId();
          idArray.add(id);
          out.write(
              new JsonObject(hit.getSourceAsString())
                  .put("ESindex", hit.index())
                  .put("EStype", hit.type())
                  .toString()
          );
          out.write("\n");
        }
        response = client.prepareSearchScroll(response.getScrollId()).setScroll(new TimeValue(scroll)).get();
        if (idArray.isEmpty()){
          continue;
        }
        for (Object o1 : idArray) {
          String id = (String) o1;
          bulkRequest.add(client.prepareDelete(index, type, id).request());
        }
        BulkResponse bulkResponse = bulkRequest.get();
        if (bulkResponse.hasFailures()) {
          for (BulkItemResponse item : bulkResponse.getItems()) {
            System.out.println(item.getFailureMessage());
          }
        }
      } while (response.getHits().getHits().length != 0);
    }
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
        .setQuery(QueryBuilders.matchAllQuery())
        .setSize(size)//max of hits will be returned for each scroll
        .setScroll(new TimeValue(scroll))//设置滚动的时间
        .setSearchType(SearchType.SCAN)//告诉ES不需要排序只要结果返回即可
        .get();

    String filePath = "es.txt";
    File file = new File(filePath);
    System.out.println(file.getAbsolutePath());
    System.out.println("delete file " + file.delete());
    System.out.println("create file " + file.createNewFile());
    try (BufferedWriter out = new BufferedWriter(new FileWriter(filePath, true))) {
      //把导出的结果以JSON的格式写到文件里,每次返回数据10000条。一直循环查询直到所有的数据都查询出来
      do {
        for (SearchHit hit : response.getHits().getHits()) {
          out.write(
              new JsonObject(hit.getSourceAsString())
                  .put("ESindex", hit.index())
                  .put("EStype", hit.type())
                  .toString()
          );
          out.write("\n");
        }
        response = client.prepareSearchScroll(response.getScrollId()).setScroll(new TimeValue(scroll)).get();
      } while (response.getHits().getHits().length != 0);
    }
  }

  private static void searchES(Client client) {
    SearchRequestBuilder requestBuilder = client
        .prepareSearch("dashboard")
        .setTypes("dashboard");

    buildQuery(requestBuilder);

    System.out.println(requestBuilder.toString());
    SearchResponse response = requestBuilder.execute().actionGet();

    System.out.println(response.getHits().totalHits());
    response.getHits().forEach(hit -> System.out.println(hit.getSourceAsString()));
  }

  private static void buildQuery(SearchRequestBuilder requestBuilder) {
    requestBuilder
        .setFrom(0)
        .setSize(5)
        .addSort("@timestamp", SortOrder.DESC);
    BoolQueryBuilder queryBuilder = QueryBuilders.boolQuery();
    QueryBuilder regexpQuery = QueryBuilders.matchQuery("operation", "账号密码登录");
    queryBuilder.must(regexpQuery);

    requestBuilder.setQuery(queryBuilder);
  }


}
