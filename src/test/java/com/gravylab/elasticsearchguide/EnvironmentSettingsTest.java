package com.gravylab.elasticsearchguide;

import org.elasticsearch.action.admin.cluster.health.ClusterHealthRequest;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.client.indices.GetIndexResponse;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.search.aggregations.bucket.MultiBucketsAggregation;
import org.elasticsearch.search.aggregations.bucket.terms.ParsedStringTerms;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

import static org.elasticsearch.index.query.QueryBuilders.termsQuery;
import static org.elasticsearch.search.aggregations.AggregationBuilders.terms;

@Testcontainers
public class EnvironmentSettingsTest extends CommonTestClass {


    @DisplayName("가상 테스트 컨테이너에서 정상 작동하는지 확인하는 테스트")
    @Test
    void test_rest_client() throws Exception {
        ClusterHealthRequest clusterHealthRequest = new ClusterHealthRequest();
        ClusterHealthResponse health = client.cluster().health(clusterHealthRequest, RequestOptions.DEFAULT);
        System.out.println("health = " + health);
    }

    @DisplayName("테스트 인덱스 생성")
    @Test
    void create_test_index() throws Exception {
        Map<String, Object> settings = new HashMap<>();
        settings.put("number_of_shards", 3);
        settings.put("number_of_replicas", 0);
        CreateIndexRequest createIndexRequest = new CreateIndexRequest("starwars")
                .settings(settings);


        CreateIndexResponse createIndexResponse = client.indices().create(createIndexRequest, RequestOptions.DEFAULT);
        System.out.println("createIndexResponse = " + createIndexResponse);
    }

    @DisplayName("스키마리스로 인덱스 생성")
    @Test
    void create_schemaless_index() throws IOException {
        IndexRequest indexRequest = new IndexRequest();
        Map<String, Object> source = new HashMap<>();
        source.put("movieCd", 1);
        source.put("movieNm", "살아남은 아이");
        source.put("movieNmEn", "Last Child");
        source.put("prdtYear", 2017);
        source.put("openDt", "");
        source.put("typeNm", "장편");
        source.put("prdtStatNm", "기타");
        source.put("nationAlt", "한국");
        source.put("genreAlt", "드라마, 가족");
        source.put("repNationNm", "한국");
        source.put("repGenreNm", "드라마");


        indexRequest.index("movie")
                .source(source);

        IndexResponse indexResponse = client.index(indexRequest, RequestOptions.DEFAULT);
        System.err.println(indexResponse.toString());

        GetIndexRequest getIndexRequest = new GetIndexRequest("movie");

        GetIndexResponse getIndexResponse = client.indices().get(getIndexRequest, RequestOptions.DEFAULT);
        System.out.println("getIndexResponse = " + getIndexResponse);
    }


    @DisplayName("movie 인덱스 생성")
    @Test
    void create_movie_index() throws IOException {
        removeIndexIfExists("movie");


        CreateIndexRequest createIndexRequest = new CreateIndexRequest("movie");

        XContentBuilder settingBuilder = makeSettingBuilder();
        XContentBuilder mappingBuilder = makeMappingBuilder();
        createIndexRequest.settings(settingBuilder);
        createIndexRequest.mapping(mappingBuilder);

        CreateIndexResponse createIndexResponse = client.indices().create(createIndexRequest, RequestOptions.DEFAULT);
        System.out.println(createIndexResponse.isAcknowledged());
    }


    @DisplayName("문서 생성하기")
    @Test
    void create_new_document() throws IOException, InterruptedException {
        if (!isExistsIndex("movie")) {
            this.create_movie_index();
        }

        IndexRequest indexRequest = new IndexRequest("movie");
        HashMap<String, Object> source = new HashMap<>();
        source.put("movieCd", 1);
        source.put("movieNm", "살아남은 아이");
        source.put("movieNmEn", "Last Child");
        source.put("prdtYear", 2017);
        source.put("openDt", "2017-10-20");
        source.put("typeNm", "장편");
        source.put("prdtStatNm", "기타");
        source.put("nationAlt", "한국");
        source.put("genreAlt", "드라마,가족");
        source.put("repNationNm", "한국");
        source.put("repGenreNm", "드라마");


        indexRequest.source(source)
                .id(String.valueOf(1));

        IndexResponse indexResponse = client.index(indexRequest, RequestOptions.DEFAULT);
        System.err.println("indexResponse = " + indexResponse);
        Thread.sleep(1000L);
        ;
    }


    @DisplayName("문서 조회하기")
    @Test
    void get_document() throws IOException, InterruptedException {
        create_new_document();

        GetRequest getRequest = new GetRequest("movie", "1");
        GetResponse getResponse = client.get(getRequest, RequestOptions.DEFAULT);
        System.err.println(getResponse.getSourceAsMap());
    }

    @DisplayName("문서 삭제")
    @Test
    void remove_document() throws IOException, InterruptedException {
        create_new_document();
        DeleteRequest deleteRequest = new DeleteRequest("movie", "1");
        DeleteResponse deleteResponse = client.delete(deleteRequest, RequestOptions.DEFAULT);
        System.err.println("deleteResponse = " + deleteResponse);
    }


    @DisplayName("문서 검색")
    @Test
    void search_document() throws IOException, InterruptedException {
        create_new_document();
        SearchRequest searchRequest = new SearchRequest("movie");
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(
                termsQuery("typeNm", "장편")
        );
        searchRequest.source(searchSourceBuilder);

        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
        System.err.println("searchResponse = " + searchResponse);
    }

    @DisplayName("데이터 집계")
    @Test
    void aggregation_data() throws IOException, InterruptedException {
        create_new_document();

        SearchRequest searchRequest = new SearchRequest();
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

        searchSourceBuilder
                .aggregation(
                        terms("group_by_genreAlt")
                                .field("genreAlt")
                );

        searchRequest.source(searchSourceBuilder);
        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);

        Map<Object, Long> result = searchResponse
                .getAggregations()
                .asList()
                .stream()
                .map(e -> ((ParsedStringTerms) e).getBuckets())
                .collect(Collectors.toList())
                .get(0)
                .stream()
                .collect(Collectors.toMap(MultiBucketsAggregation.Bucket::getKey, MultiBucketsAggregation.Bucket::getDocCount));

        System.out.println("result = " + result);

        //장르별 국가 형태를 중첩해서 보여주는 집계
        searchRequest = new SearchRequest("movie");
        searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.aggregation(
                terms("genre")
                        .field("genreAlt")
                        .subAggregation(
                                terms("nation")
                                        .field("nationAlt")
                        )
        );
        searchRequest.source(searchSourceBuilder);

        searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
        System.out.println("searchResponse = " + searchResponse);
    }


    private void defineField(XContentBuilder mappingBuilder, String field, String type) throws IOException {
        mappingBuilder.startObject(field);
        {
            mappingBuilder.field("type", type);
        }
        mappingBuilder.endObject();
    }


    private XContentBuilder makeMappingBuilder() throws IOException {
        XContentBuilder mappingBuilder = XContentFactory.jsonBuilder();
        mappingBuilder.startObject();
        {
            mappingBuilder.startObject("properties");
            {
                defineField(mappingBuilder, "movieCd", "integer");
                defineField(mappingBuilder, "movieNm", "text");
                defineField(mappingBuilder, "movieNmEn", "text");
                defineField(mappingBuilder, "prdtYear", "integer");
                defineField(mappingBuilder, "openDt", "date");
                defineField(mappingBuilder, "typeNm", "keyword");
                defineField(mappingBuilder, "prdtStatNm", "keyword");
                defineField(mappingBuilder, "nationAlt", "keyword");
                defineField(mappingBuilder, "genreAlt", "keyword");
                defineField(mappingBuilder, "repNationNm", "keyword");
                defineField(mappingBuilder, "repGenreNm", "keyword");
            }
            mappingBuilder.endObject();
        }
        mappingBuilder.endObject();
        return mappingBuilder;
    }

    private XContentBuilder makeSettingBuilder() throws IOException {
        XContentBuilder settingBuilder = XContentFactory.jsonBuilder();
        settingBuilder.startObject();
        {
            settingBuilder.field("number_of_shards", 3);
            settingBuilder.field("number_of_replicas", 2);
        }
        settingBuilder.endObject();
        return settingBuilder;
    }
}
