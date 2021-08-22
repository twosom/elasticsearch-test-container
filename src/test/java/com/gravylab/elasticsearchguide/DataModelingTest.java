package com.gravylab.elasticsearchguide;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.lucene.search.join.ScoreMode;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.indices.*;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.DeleteByQueryRequest;
import org.elasticsearch.index.reindex.ReindexRequest;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.bucket.MultiBucketsAggregation;
import org.elasticsearch.search.aggregations.bucket.terms.ParsedStringTerms;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.core.io.Resource;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;

import java.io.IOException;
import java.io.InputStream;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static org.elasticsearch.index.query.QueryBuilders.*;
import static org.elasticsearch.search.aggregations.AggregationBuilders.terms;
import static org.junit.jupiter.api.Assertions.*;

public class DataModelingTest extends CommonTestClass {


    public static final String NUMBER_OF_SHARDS = "number_of_shards";
    public static final String NUMBER_OF_REPLICAS = "number_of_replicas";
    public static final String MOVIE_SEARCH = "movie_search";
    public static final String PETITIONS = "petitions";


    @DisplayName("매핑 실패 테스트")
    @Test
    void mapping_fail_test() throws IOException {
        removeIndexIfExists("movie");
        CreateIndexRequest createIndexRequest = new CreateIndexRequest("movie");
        CreateIndexResponse createIndexResponse = testContainerClient.indices().create(createIndexRequest, RequestOptions.DEFAULT);

        IndexRequest request1 = new IndexRequest("movie");
        Map<String, Object> source = new HashMap<>();
        source.put("movieCd", 20173732);
        source.put("movieNm", "캡틴 아메리카");
        request1.source(source);

        testContainerClient.index(request1, RequestOptions.DEFAULT);


        IndexRequest request2 = new IndexRequest("movie");
        source = new HashMap<>();
        source.put("movieCd", "XT001");
        source.put("movieNm", "아이언맨");

        // 실패를 해야 성공한다.
        assertThrows(ActionRequestValidationException.class, () -> {
            testContainerClient.index(request2, RequestOptions.DEFAULT);
        });
    }


    @DisplayName("인덱스 생성")
    @Test
    void create_index_test() throws IOException {
        removeIndexIfExists(MOVIE_SEARCH);

        CreateIndexRequest createIndexRequest = new CreateIndexRequest(MOVIE_SEARCH);
        XContentBuilder settingBuilder = createSettingBuilder();
        XContentBuilder mappingBuilder = createMappingBuilder();
        createIndexRequest.settings(settingBuilder)
                .mapping(mappingBuilder);


        CreateIndexResponse createIndexResponse = testContainerClient.indices().create(createIndexRequest, RequestOptions.DEFAULT);

        assertCreatedIndex(createIndexResponse, MOVIE_SEARCH);
    }

    @DisplayName("매핑 확인")
    @Test
    void check_mapping_api() throws Exception {
        // 인덱스가 생성되지 않은 경우 다시 생성
        if (!isExistsIndex(MOVIE_SEARCH, testContainerClient)) {
            create_index_test();
        }


        GetMappingsRequest getMappingsRequest = new GetMappingsRequest();
        getMappingsRequest.indices(MOVIE_SEARCH);


        GetMappingsResponse getMappingsResponse = testContainerClient.indices().getMapping(getMappingsRequest, RequestOptions.DEFAULT);

        Map<String, Object> mappingMetaDataMap = getMappingsResponse.mappings()
                .get(MOVIE_SEARCH)
                .getSourceAsMap();

        System.out.println("===============MAPPING_METADATA================");
        for (String s : mappingMetaDataMap.keySet()) {
            System.err.println(s + " : " + mappingMetaDataMap.get(s).toString() + "\n");
        }
    }


    @DisplayName("다중 필드 설정 테스트")
    @Test
    void put_mapping_test() throws Exception {
        if (!isExistsIndex(MOVIE_SEARCH, testContainerClient)) {
            create_index_test();
        }
        PutMappingRequest putMappingRequest = new PutMappingRequest(MOVIE_SEARCH);

        XContentBuilder builder = XContentFactory.jsonBuilder();


        builder.startObject();
        {
            builder.startObject("properties");
            {
                builder.startObject("awards");
                {
                    builder.field("type", "text");
                    builder.startObject("fields");
                    {
                        builder.startObject("name");
                        {
                            builder.field("type", "keyword");
                        }
                        builder.endObject();
                    }
                    builder.endObject();
                }
                builder.endObject();
            }
            builder.endObject();
        }
        builder.endObject();

        putMappingRequest.source(builder);
        AcknowledgedResponse putMappingResponse = testContainerClient.indices().putMapping(putMappingRequest, RequestOptions.DEFAULT);
        assertTrue(putMappingResponse.isAcknowledged());
    }

    @DisplayName("null_value 매핑 테스트")
    @Test
    void null_value_mapping_test() throws Exception {
        if (!isExistsIndex(MOVIE_SEARCH, testContainerClient)) {
            create_index_test();
        }


        PutMappingRequest putMappingRequest = new PutMappingRequest(MOVIE_SEARCH);
        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();
        {
            builder.startObject("properties");
            {
                builder.startObject("audiCnt");
                {
                    builder.field("type", "integer");
                    builder.field("null_value", "0");
                }
                builder.endObject();
            }
            builder.endObject();
        }
        builder.endObject();

        putMappingRequest.source(builder);

        AcknowledgedResponse putMappingResponse = testContainerClient.indices().putMapping(putMappingRequest, RequestOptions.DEFAULT);
        assertTrue(putMappingResponse.isAcknowledged());
    }


    @DisplayName("Aggregation 을 위한 SampleData 삽입")
    @Test
    void index_for_aggregation() throws Exception {
        PathMatchingResourcePatternResolver resolver = new PathMatchingResourcePatternResolver();
        Resource[] resources = resolver.getResources("classpath:sample-json/petitions*.json");
        System.out.println("resources.length = " + resources.length);
        List<List<Map<String, Object>>> collect = Arrays.stream(resources)
                .map(this::getInputStream)
                .map(this::readValue)
                .collect(Collectors.toList());

        List<Map<String, Object>> sources = collect
                .stream().flatMap(Collection::stream)
                .collect(Collectors.toList());


        if (isExistsIndex(PETITIONS, dockerClient)) {
            AcknowledgedResponse deleteIndexResponse = dockerClient.indices().delete(new DeleteIndexRequest(PETITIONS), RequestOptions.DEFAULT);
            assertTrue(deleteIndexResponse.isAcknowledged());
        }

        //TODO 인덱스 생성
        CreateIndexRequest createIndexRequest = new CreateIndexRequest(PETITIONS);

        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();
        {
            builder.startObject("properties");
            {
                defineField(builder, "category", "keyword");
                defineField(builder, "begin", "date");
                defineField(builder, "end", "date");
                defineField(builder, "content", "text");
                defineField(builder, "num_agree", "integer");
                defineField(builder, "position_idx", "integer");
                defineField(builder, "status", "keyword");
                defineField(builder, "title", "text");
            }
            builder.endObject();
        }
        builder.endObject();

        createIndexRequest.mapping(builder);
        CreateIndexResponse createIndexResponse = dockerClient.indices().create(createIndexRequest, RequestOptions.DEFAULT);
        assertCreatedIndex(createIndexResponse, PETITIONS);

        BulkRequest bulkRequest = new BulkRequest();
        int i = 0;
        for (Map<String, Object> source : sources) {
            IndexRequest indexRequest = new IndexRequest(PETITIONS);
            indexRequest.source(source);
            bulkRequest.add(indexRequest);
            i++;
            if (i >= 10000) {
                i = 0;
                long count = Arrays.stream(dockerClient.bulk(bulkRequest, RequestOptions.DEFAULT)
                                .getItems())
                        .filter(e -> e.getOpType().equals(DocWriteRequest.OpType.INDEX))
                        .count();
                System.out.println(count + " 개의 문서가 인덱싱 되었습니다.");
                bulkRequest = new BulkRequest();
            }
        }
        BulkResponse bulkResponse = dockerClient.bulk(bulkRequest, RequestOptions.DEFAULT);
        System.out.println(bulkResponse.getIngestTookInMillis());

        Thread.sleep(2000);

        SearchRequest searchRequest = new SearchRequest(PETITIONS);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(
                matchAllQuery()
        );


        searchRequest.source(searchSourceBuilder);


        SearchResponse searchResponse = dockerClient.search(searchRequest, RequestOptions.DEFAULT);

        List<Map<String, Object>> collect1 = Arrays.stream(searchResponse.getInternalResponse()
                        .hits()
                        .getHits())
                .map(SearchHit::getSourceAsMap)
                .collect(Collectors.toList());

        System.out.println("collect1 = " + collect1);
    }


    @DisplayName("_type 메타 필드로 aggregation")
    @Test
    void aggregation_with_type_field() throws Exception {
        if (!isExistsIndex("movie_search", testContainerClient)) {
            create_index_test();

        }


        SearchRequest searchRequest = new SearchRequest("movie_search");

        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.aggregation(
                terms("indices")
                        .field("_type")
                        .size(10)
        );


        searchRequest.source(searchSourceBuilder);


        SearchResponse searchResponse = testContainerClient.search(searchRequest, RequestOptions.DEFAULT);
        System.out.println("searchResponse = " + searchResponse);
    }


    @DisplayName("petitions category 별로 집계 실행")
    @Test
    void aggregation_by_category() throws Exception {
        if (!isExistsIndex(PETITIONS, dockerClient)) {
            index_for_aggregation();
        }

        SearchRequest searchRequest = new SearchRequest(PETITIONS);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

        searchSourceBuilder.aggregation(
                terms("aggregate_by_category")
                        .field("category")
        );


        searchRequest.source(searchSourceBuilder);

        SearchResponse searchResponse = dockerClient.search(searchRequest, RequestOptions.DEFAULT);

        Map<Object, Long> aggregationResult = ((ParsedStringTerms) searchResponse
                .getAggregations()
                .asList()
                .get(0))
                .getBuckets()
                .stream()
                .collect(Collectors.toMap(MultiBucketsAggregation.Bucket::getKey, MultiBucketsAggregation.Bucket::getDocCount));

        System.out.println("aggregationResult = " + aggregationResult);
    }

    @DisplayName("reindex API 테스트")
    @Test
    void reindex_test() throws Exception {
        if (!isExistsIndex(PETITIONS, dockerClient)) {
            index_for_aggregation();
        }

        removeIndexIfExists("reindex_petitions");


        ReindexRequest reindexRequest = new ReindexRequest();
        reindexRequest.setTimeout(TimeValue.timeValueDays(1));
        reindexRequest.setSourceIndices(PETITIONS)
                .setDestIndex("reindex_petitions");

        Script script = new Script(ScriptType.INLINE, "painless", "ctx._source.petition_idx++", new HashMap<>());


        reindexRequest.setScript(script);

        BulkByScrollResponse reindexResponse
                = dockerClient.reindex(reindexRequest, RequestOptions.DEFAULT);

        System.out.println("reindexResponse = " + reindexResponse);


    }


    @DisplayName("_routing 테스트")
    @Test
    void routing_test() throws Exception {
        // 인덱스가 생성되지 않은 경우 다시 생성
        if (!isExistsIndex(MOVIE_SEARCH, testContainerClient)) {
            create_index_test();
        }

        IndexRequest indexRequest = new IndexRequest(MOVIE_SEARCH);

        Map<String, Object> source = new HashMap<>();
        source.put("repGenreNm", "한국어");
        source.put("movieNm", "살아남은 아이");
        indexRequest.routing("ko")
                .source(source);

        IndexResponse indexResponse = testContainerClient.index(indexRequest, RequestOptions.DEFAULT);
        Thread.sleep(2000);


        SearchRequest searchRequest = new SearchRequest(MOVIE_SEARCH);
        searchRequest.routing("ko");

        SearchResponse searchResponse = testContainerClient.search(searchRequest, RequestOptions.DEFAULT);
        System.out.println("searchResponse = " + searchResponse);
    }

    @DisplayName("칼럼에 Array 데이터 타입을 사용하는 예")
    @Test
    void index_with_array_data_type() throws Exception {
        // 인덱스가 생성되지 않은 경우 다시 생성
        if (!isExistsIndex(MOVIE_SEARCH, testContainerClient)) {
            create_index_test();
        }

        IndexRequest indexRequest = new IndexRequest(MOVIE_SEARCH);
        Map<String, Object> source = new HashMap<>();
        source.put("title", "해리포터와 마법사의 돌");
        source.put("subtitleLang", new String[]{"ko", "en"});
        indexRequest.source(source);

        IndexResponse indexResponse = testContainerClient.index(indexRequest, RequestOptions.DEFAULT);

        System.out.println("indexResponse = " + indexResponse);
    }


    @DisplayName("date_range 타입 정의")
    @Test
    void date_range_test() throws Exception {
        // 인덱스가 생성되지 않은 경우 다시 생성
        if (!isExistsIndex(MOVIE_SEARCH, testContainerClient)) {
            create_index_test();
        }
        PutMappingRequest putMappingRequest = new PutMappingRequest(MOVIE_SEARCH);

        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();
        {
            builder.startObject("properties");
            {
                builder.startObject("showRange");
                {
                    builder.field("type", "date_range");
                }
                builder.endObject();
            }
            builder.endObject();
        }
        builder.endObject();
        putMappingRequest.source(builder);

        AcknowledgedResponse putMappingResponse
                = testContainerClient.indices().putMapping(putMappingRequest, RequestOptions.DEFAULT);


        assertTrue(putMappingResponse.isAcknowledged());


        IndexRequest indexRequest = new IndexRequest(MOVIE_SEARCH);
        builder = XContentFactory.jsonBuilder();
        builder.startObject();
        {
            builder.startObject("showRange");
            {
                builder.field("gte", "2001-01-01");
                builder.field("lte", "2001-12-31");
            }
            builder.endObject();
        }
        builder.endObject();

        indexRequest.source(builder);

        IndexResponse indexResponse = testContainerClient.index(indexRequest, RequestOptions.DEFAULT);
        System.out.println("indexResponse = " + indexResponse);
    }


    @DisplayName("Geo-Point 데이터 타입 매핑")
    @Test
    void geo_data_type_mapping_test() throws Exception {
        // 인덱스가 생성되지 않은 경우 다시 생성
        if (!isExistsIndex(MOVIE_SEARCH, testContainerClient)) {
            create_index_test();
        }

        PutMappingRequest putMappingRequest = new PutMappingRequest(MOVIE_SEARCH);
        XContentBuilder builder = XContentFactory.jsonBuilder();

        builder.startObject();
        {
            builder.startObject("properties");
            {
                builder.startObject("filmLocation");
                {
                    builder.field("type", "geo_point");
                }
                builder.endObject();
            }
            builder.endObject();
        }
        builder.endObject();

        putMappingRequest.source(builder);

        AcknowledgedResponse putMappingResponse = testContainerClient.indices().putMapping(putMappingRequest, RequestOptions.DEFAULT);
        assertTrue(putMappingResponse.isAcknowledged());


        IndexRequest indexRequest = new IndexRequest(MOVIE_SEARCH);
        builder = XContentFactory.jsonBuilder();
        builder.startObject();
        {
            builder.field("title", "해리포터와 마법사의 돌");
            builder.startObject("filmLocation");
            {
                builder.field("lat", 55.4155828);
                builder.field("lon", -1.7081091);
            }
            builder.endObject();
        }
        builder.endObject();

        indexRequest.source(builder);

        IndexResponse indexResponse = testContainerClient.index(indexRequest, RequestOptions.DEFAULT);
        System.out.println("indexResponse = " + indexResponse);

        Thread.sleep(2000);

        SearchRequest searchRequest = new SearchRequest(MOVIE_SEARCH);
        SearchResponse searchResponse = testContainerClient.search(searchRequest, RequestOptions.DEFAULT);
        System.out.println("searchResponse = " + searchResponse);
    }

    @DisplayName("IP 데이터 타입 매핑")
    @Test
    void ip_data_type_mapping() throws Exception {
        // 인덱스가 생성되지 않은 경우 다시 생성
        if (!isExistsIndex(MOVIE_SEARCH, testContainerClient)) {
            create_index_test();
        }


        PutMappingRequest putMappingRequest = new PutMappingRequest(MOVIE_SEARCH);
        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();
        {
            builder.startObject("properties");
            {
                builder.startObject("ipAddr");
                {
                    builder.field("type", "ip");
                }
                builder.endObject();
            }
            builder.endObject();
        }
        builder.endObject();


        putMappingRequest.source(builder);

        AcknowledgedResponse putMappingResponse = testContainerClient.indices().putMapping(putMappingRequest, RequestOptions.DEFAULT);
        assertTrue(putMappingResponse.isAcknowledged());

        IndexRequest indexRequest = new IndexRequest(MOVIE_SEARCH);
        builder = XContentFactory.jsonBuilder();
        builder.startObject();
        {
            builder.field("ipAddr", "127.0.0.1");
        }
        builder.endObject();


        indexRequest.source(builder);
        IndexResponse indexResponse = testContainerClient.index(indexRequest, RequestOptions.DEFAULT);

        System.out.println("indexResponse = " + indexResponse);


    }


    @DisplayName("Object 데이터 타입 매핑")
    @Test
    void object_data_type_mapping() throws Exception {
        // 인덱스가 생성되지 않은 경우 다시 생성
        if (!isExistsIndex(MOVIE_SEARCH, testContainerClient)) {
            create_index_test();
        }


        PutMappingRequest putMappingRequest = new PutMappingRequest(MOVIE_SEARCH);
        XContentBuilder builder = XContentFactory.jsonBuilder();

        builder.startObject();
        {
            builder.startObject("properties");
            {
                builder.startObject("companies");
                {
                    builder.startObject("properties");
                    {
                        builder.startObject("companyName");
                        {
                            builder.field("type", "text");
                        }
                        builder.endObject();
                    }
                    builder.endObject();
                }
                builder.endObject();
            }
            builder.endObject();
        }
        builder.endObject();


        putMappingRequest.source(builder);
        AcknowledgedResponse putMappingResponse = testContainerClient.indices().putMapping(putMappingRequest, RequestOptions.DEFAULT);
        assertTrue(putMappingResponse.isAcknowledged());


        IndexRequest indexRequest = new IndexRequest(MOVIE_SEARCH);

        builder = XContentFactory.jsonBuilder();
        builder.startObject();
        {
            builder.field("title", "해리포터와 마법사의 돌");
            builder.startObject("companies");
            {
                builder.field("companyName", "워너브라더스");
            }
            builder.endObject();
        }
        builder.endObject();
        indexRequest.source(builder);


        IndexResponse indexResponse = testContainerClient.index(indexRequest, RequestOptions.DEFAULT);
        System.out.println("indexResponse = " + indexResponse);

        // Object 타입을 배열 형태로도 저장할 수 있다.
        indexRequest = new IndexRequest(MOVIE_SEARCH);
        builder = XContentFactory.jsonBuilder();
        builder.startObject();
        {
            builder.field("title", "해리포터와 마법사의 돌");
            builder.startArray("companies");
            {
                builder.startObject();
                {
                    builder.field("companyName", "워너브라더스");
                }
                builder.endObject();
                builder.startObject();
                {
                    builder.field("companyName", "Heyday Films");
                }
                builder.endObject();
            }
            builder.endArray();
        }
        builder.endObject();
        indexRequest.source(builder);
        indexResponse = testContainerClient.index(indexRequest, RequestOptions.DEFAULT);
        System.out.println("indexResponse = " + indexResponse);
    }


    @DisplayName("nested 데이터 타입 매핑")
    @Test
    void nested_data_type_mapping() throws Exception {
        // 인덱스가 생성되지 않은 경우 다시 생성
        if (!isExistsIndex(MOVIE_SEARCH, testContainerClient)) {
            create_index_test();
        }

        PutMappingRequest putMappingRequest = new PutMappingRequest(MOVIE_SEARCH);
        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();
        {
            builder.startObject("properties");
            {
                builder.startObject("companies_nested");
                {
                    builder.field("type", "nested");
                }
                builder.endObject();
            }
            builder.endObject();
        }
        builder.endObject();


        putMappingRequest.source(builder);


        AcknowledgedResponse putMappingResponse = testContainerClient.indices().putMapping(putMappingRequest, RequestOptions.DEFAULT);
        assertTrue(putMappingResponse.isAcknowledged());

        IndexRequest indexRequest = new IndexRequest(MOVIE_SEARCH);
        builder = XContentFactory.jsonBuilder();

        builder.startObject();
        {
            builder.field("title", "해리포터와 마법사의 돌");
            builder.startArray("companies_nested");
            {
                builder.startObject();
                {
                    builder.field("companyCd", "1");
                    builder.field("companyName", "워너브라더스");
                }
                builder.endObject();
                builder.startObject();
                {
                    builder.field("companyCd", "2000");
                    builder.field("companyName", "Heyday Films");
                }
                builder.endObject();
            }
            builder.endArray();
        }
        builder.endObject();


        indexRequest.source(builder);


        IndexResponse indexResponse = testContainerClient.index(indexRequest, RequestOptions.DEFAULT);
        System.out.println("indexResponse = " + indexResponse);
        Thread.sleep(2000);

        //== 문제의 쿼리 다시 실행 ==//
        SearchRequest searchRequest = new SearchRequest(MOVIE_SEARCH);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(
                nestedQuery("companies_nested",
                        boolQuery()
                                .must(
                                        matchQuery("companies_nested.companyName", "워너브라더스")
                                )
                                .must(
                                        matchQuery("companies_nested.companyCd", "2")
                                ),
                        ScoreMode.None
                )
        );


        searchRequest.source(searchSourceBuilder);

        SearchResponse searchResponse = testContainerClient.search(searchRequest, RequestOptions.DEFAULT);
        System.out.println("searchResponse = " + searchResponse);
    }


    @DisplayName("Analyze API 사용")
    @Test
    void use_analyze_api() throws Exception {
        AnalyzeRequest analyzeRequest = AnalyzeRequest.withGlobalAnalyzer("standard", "우리나라가 좋은나라, 대한미눅 화이팅");
        AnalyzeResponse analyzeResponse = testContainerClient.indices().analyze(analyzeRequest, RequestOptions.DEFAULT);
        System.out.println("analyzeResponse = " + analyzeResponse);
    }

    @DisplayName("HTML 태그 제거하는 Analyzer 가 적용된 index 생성하기")
    @Test
    void create_index_with_html_tag_analyzer() throws Exception {
        CreateIndexRequest createIndexRequest = new CreateIndexRequest("movie_analyzer");
        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();
        {
            builder.startObject("settings");
            {
                builder.startObject("index");
                {
                    builder.field(NUMBER_OF_SHARDS, 5);
                    builder.field(NUMBER_OF_REPLICAS, 1);
                }
                builder.endObject();
            }
            builder.endObject();
            builder.startObject("analysis");
            {
                builder.startObject("analyzer");
                {
                    builder.startObject("custom_movie_analyzer");
                    {
                        builder.field("type", "custom");
                        //TODO
                        // Character Filter 를 정의했다. 전체 텍스트 문장에서 HTML 태그를 제거한다.
                        builder.startArray("char_filter");
                        {
                            builder.value("html_strip");
                        }
                        builder.endArray();
                        //TODO
                        // Tokenizer Filter 를 정의했다. 특수문자 혹은 공백을 기준으로 텍스트를 분할한다.
                        builder.field("tokenizer", "standard");
                        builder.startArray("filter");
                        {
                            //TODO
                            // Token Filter 를 정의했다. 모든 토큰을 소문자로 변환한다.
                            builder.value("lowercase");
                        }
                        builder.endArray();
                    }
                    builder.endObject();
                }
                builder.endObject();
            }
            builder.endObject();
        }
        builder.endObject();

        //TODO
        // 1. html_strip 에 의해 HTML 태그 제거
        // 2. Standard 토크나이저로 Term 분리
        // 3. lowercase 필터로 소문자 처리


        createIndexRequest.source(builder);
        CreateIndexResponse createIndexResponse = testContainerClient.indices().create(createIndexRequest, RequestOptions.DEFAULT);
        assertTrue(createIndexResponse.isAcknowledged());
        System.out.println("createIndexResponse = " + createIndexResponse);
    }


    @DisplayName("분석기를 이용한 분석")
    @Test
    void analyze_test() throws Exception {
        AnalyzeRequest analyzeRequest = AnalyzeRequest.withGlobalAnalyzer("standard", "캐리비안의 해적");
        AnalyzeResponse analyzeResponse = testContainerClient.indices().analyze(analyzeRequest, RequestOptions.DEFAULT);
        System.out.println("analyzeResponse = " + analyzeResponse);


        // 인덱스가 생성되지 않은 경우 다시 생성
        if (!isExistsIndex("movie_analyzer", testContainerClient)) {
            create_index_with_html_tag_analyzer();
        }


        AnalyzeRequest request = AnalyzeRequest.withField("movie_analyzer", "title", "캐리비안의 해적");
        AnalyzeResponse analyze = testContainerClient.indices().analyze(request, RequestOptions.DEFAULT);
        System.out.println("analyze = " + analyze);
    }

    @DisplayName("색인과 검색 시 분석기를 각각 설정")
    @Test
    void create_index_with_analyzer_aggregated() throws Exception {
        removeIndexIfExists("movie_analyzer");

        CreateIndexRequest createIndexRequest = new CreateIndexRequest("movie_analyzer");
        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();
        {
            builder.startObject("settings");
            {
                builder.startObject("index");
                {
                    builder.field(NUMBER_OF_SHARDS, 5);
                    builder.field(NUMBER_OF_REPLICAS, 1);
                }
                builder.endObject();
                builder.startObject("analysis");
                {
                    builder.startObject("analyzer");
                    {
                        builder.startObject("movie_lower_test_analyzer");
                        {
                            builder.field("type", "custom");
                            builder.field("tokenizer", "standard");
                            builder.startArray("filter");
                            {
                                builder.value("lowercase");
                            }
                            builder.endArray();
                        }
                        builder.endObject();
                        builder.startObject("movie_stop_test_analyzer");
                        {
                            builder.field("type", "custom");
                            builder.field("tokenizer", "standard");
                            builder.startArray("filter");
                            {
                                builder.value("lowercase");
                                builder.value("english_stop");
                            }
                            builder.endArray();
                        }
                        builder.endObject();
                    }
                    builder.endObject();
                }
                builder.endObject();
                builder.startObject("filter");
                {
                    builder.startObject("english_stop");
                    {
                        builder.field("type", "stop");
                        builder.field("stopwords", "_english_");
                    }
                    builder.endObject();
                }
                builder.endObject();
                builder.startObject("mappings");
                {
                    builder.startObject("_doc");
                    {
                        builder.startObject("properties");
                        {
                            builder.startObject("title");
                            {
                                builder.field("type", "text");
                                builder.field("analyzer", "movie_stop_test_analyzer");
                                builder.field("search_analyzer", "movie_lower_test_analyzer");
                            }
                            builder.endObject();
                        }
                        builder.endObject();
                    }
                    builder.endObject();
                }
                builder.endObject();
            }
            builder.endObject();

        }
        builder.endObject();

        createIndexRequest.source(builder);


        CreateIndexResponse createIndexResponse = testContainerClient.indices().create(createIndexRequest, RequestOptions.DEFAULT);
        assertTrue(createIndexResponse.isAcknowledged());
        System.out.println("createIndexResponse = " + createIndexResponse);
    }


    @DisplayName("여러가지 Analyzer 를 이용하여 분석")
    @ParameterizedTest(name = "analyzer : [{0}]")
    @ValueSource(strings = {"standard", "whitespace", "keyword"})
    void analyze_with_standard_analyzer(String analyzer) throws Exception {
        AnalyzeRequest analyzeRequest = AnalyzeRequest.withGlobalAnalyzer(analyzer, "Harry Potter and the Chamber of Secrets");
        AnalyzeResponse analyzeResponse = testContainerClient.indices().analyze(analyzeRequest, RequestOptions.DEFAULT);
        List<String> collect = analyzeResponse.getTokens()
                .stream()
                .map(AnalyzeResponse.AnalyzeToken::getTerm)
                .collect(Collectors.toList());
        System.out.println(collect);
    }

    @DisplayName("전처리 필터 테스트를 위한 인덱스 생성")
    @Test
    void create_movie_html_analyzer_index() throws Exception {
        CreateIndexRequest createIndexRequest = new CreateIndexRequest("movie_html_analyzer");
        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();
        {
            builder.startObject("settings");
            {
                builder.startObject("analysis");
                {
                    builder.startObject("analyzer");
                    {
                        builder.startObject("html_strip_analyzer");
                        {
                            builder.field("tokenizer", "keyword");
                            builder.startArray("char_filter");
                            {
                                builder.value("html_strip_char_filter");
                            }
                            builder.endArray();
                        }
                        builder.endObject();
                    }
                    builder.endObject();
                    builder.startObject("char_filter");
                    {
                        builder.startObject("html_strip_char_filter");
                        {
                            builder.field("type", "html_strip");
                            //TODO
                            // escaped_tags 옵션을 사용하여 제거되는 태그의 종류 선택 가능
                            builder.startArray("escaped_tags");
                            {
                                builder.value("b");
                            }
                            builder.endArray();
                        }
                        builder.endObject();
                    }
                    builder.endObject();

                }
                builder.endObject();
            }
            builder.endObject();
        }
        builder.endObject();


        createIndexRequest.source(builder);

        CreateIndexResponse createIndexResponse = testContainerClient.indices().create(createIndexRequest, RequestOptions.DEFAULT);
        assertCreatedIndex(createIndexResponse, "movie_html_analyzer");


        //TODO 생성한 인덱스에서 HTML 이 포함된 문장을 입력해서 HTML 태그가 잘 제거되는지 확인해보기
        AnalyzeRequest analyzeRequest =
                AnalyzeRequest.withIndexAnalyzer("movie_html_analyzer", "html_strip_analyzer", "<span>Harry Potter</span> and the <b>Chamber</b> of Secrets");


        AnalyzeResponse analyzeResponse = testContainerClient.indices().analyze(analyzeRequest, RequestOptions.DEFAULT);
        List<String> collect = analyzeResponse.getTokens()
                .stream()
                .map(AnalyzeResponse.AnalyzeToken::getTerm)
                .collect(Collectors.toList());
        System.out.println("collect = " + collect);
    }


    @DisplayName("다양한 토크나이저를 이용해서 분석")
    @ParameterizedTest(name = "tokenizer : [{0}]")
    @ValueSource(strings = {"standard", "whitespace"})
    void analyze_with_tokenizer(String tokenizer) throws Exception {
        AnalyzeRequest analyzeRequest = AnalyzeRequest.buildCustomAnalyzer(tokenizer)
                .build("Harry Potter and the Chamber of Secrets");

        AnalyzeResponse analyzeResponse = testContainerClient.indices().analyze(analyzeRequest, RequestOptions.DEFAULT);
        List<String> collect = analyzeResponse.getTokens()
                .stream()
                .map(AnalyzeResponse.AnalyzeToken::getTerm)
                .collect(Collectors.toList());
        System.out.println("collect = " + collect);
    }


    @DisplayName("Ngram 토크나이저를 이용해서 분석")
    @Test
    void analyze_with_ngram_tokenizer() throws Exception {
        //TODO Ngram 토크나이저를 테스트하기 위해 인덱스 생성
        CreateIndexRequest createIndexRequest = new CreateIndexRequest("movie_ngram_analyzer");
        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();
        {
            builder.startObject("analysis");
            {
                builder.startObject("analyzer");
                {
                    builder.startObject("ngram_analyzer");
                    {
                        builder.field("tokenizer", "ngram_tokenizer");
                    }
                    builder.endObject();
                }
                builder.endObject();
                builder.startObject("tokenizer");
                {
                    builder.startObject("ngram_tokenizer");
                    {
                        builder.field("type", "ngram");
                        builder.field("min_gram", 3);
                        builder.field("max_gram", 3);
                        builder.startArray("token_chars");
                        {
                            builder.value("letter");
                        }
                        builder.endArray();
                    }
                    builder.endObject();
                }
                builder.endObject();
            }
            builder.endObject();
        }
        builder.endObject();

        createIndexRequest.settings(builder);


        CreateIndexResponse createIndexResponse = testContainerClient.indices().create(createIndexRequest, RequestOptions.DEFAULT);
        assertCreatedIndex(createIndexResponse, "movie_ngram_analyzer");

        //TODO Ngram 토크나이저를 이용해 문장 분리하기
        AnalyzeRequest analyzeRequest = AnalyzeRequest.buildCustomAnalyzer("movie_ngram_analyzer", "ngram_tokenizer")
                .build("Harry Potter and the Chamber of Secrets");


        AnalyzeResponse analyzeResponse = testContainerClient.indices().analyze(analyzeRequest, RequestOptions.DEFAULT);
        List<String> collect = analyzeResponse.getTokens()
                .stream()
                .map(AnalyzeResponse.AnalyzeToken::getTerm)
                .collect(Collectors.toList());

        System.out.println("collect = " + collect);
    }

    @DisplayName("Edge Ngram 토크나이저를 이용해서 분석")
    @Test
    void analyze_with_edge_ngram_tokenizer() throws Exception {
        //TODO Edge Ngram 토크나이저를 테스트하기 위해 인덱스 생성
        CreateIndexRequest createIndexRequest = new CreateIndexRequest("movie_engram_analyzer");
        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();
        {
            builder.startObject("analysis");
            {
                builder.startObject("analyzer");
                {
                    builder.startObject("edge_ngram_analyzer");
                    {
                        builder.field("tokenizer", "edge_ngram_tokenizer");
                    }
                    builder.endObject();
                }
                builder.endObject();

                builder.startObject("tokenizer");
                {
                    builder.startObject("edge_ngram_tokenizer");
                    {
                        builder.field("type", "edge_ngram");
                        builder.field("min_gram", 2);
                        builder.field("max_gram", 10);
                        builder.startArray("token_chars");
                        {
                            builder.value("letter");
                        }
                        builder.endArray();
                    }
                    builder.endObject();
                }
                builder.endObject();
            }
            builder.endObject();
        }
        builder.endObject();


        createIndexRequest.settings(builder);

        CreateIndexResponse createIndexResponse = testContainerClient.indices().create(createIndexRequest, RequestOptions.DEFAULT);
        assertCreatedIndex(createIndexResponse, "movie_engram_analyzer");

        //TODO Edge Ngram 토크나이저를 이용해 문장 분리
        AnalyzeRequest analyzeRequest = AnalyzeRequest.buildCustomAnalyzer("movie_engram_analyzer", "edge_ngram_tokenizer")
                .build("Harry Potter and the Chamber of Secrets");

        AnalyzeResponse analyzeResponse = testContainerClient.indices().analyze(analyzeRequest, RequestOptions.DEFAULT);
        List<String> collect = analyzeResponse.getTokens()
                .stream()
                .map(AnalyzeResponse.AnalyzeToken::getTerm)
                .collect(Collectors.toList());

        System.out.println("collect = " + collect);
    }


    @DisplayName("Ascii Folding 토큰 필터를 이용해서 분석")
    @Test
    void analyze_with_ascii_folding_token_filter() throws Exception {
        //TODO Ascii Folding 토큰 필터를 테스트하기 위해 인덱스 생성
        CreateIndexRequest createIndexRequest = new CreateIndexRequest("movie_af_analyzer");
        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();
        {
            builder.startObject("analysis");
            {
                builder.startObject("analyzer");
                {
                    builder.startObject("asciifolding_analyzer");
                    {
                        builder.field("tokenizer", "standard");
                        builder.startArray("filter");
                        {
                            builder.value("lowercase");
                            builder.value("asciifolding");
                        }
                        builder.endArray();
                    }
                    builder.endObject();
                }
                builder.endObject();
            }
            builder.endObject();
        }
        builder.endObject();


        createIndexRequest.settings(builder);

        CreateIndexResponse createIndexResponse = testContainerClient.indices().create(createIndexRequest, RequestOptions.DEFAULT);
        assertCreatedIndex(createIndexResponse, "movie_af_analyzer");

        AnalyzeRequest analyzeRequest = AnalyzeRequest.withIndexAnalyzer("movie_af_analyzer", "asciifolding_analyzer", "hello javacåfe");
        AnalyzeResponse analyzeResponse = testContainerClient.indices().analyze(analyzeRequest, RequestOptions.DEFAULT);
        List<String> collect = analyzeResponse.getTokens()
                .stream()
                .map(AnalyzeResponse.AnalyzeToken::getTerm)
                .collect(Collectors.toList());
        System.out.println("collect = " + collect);
    }


    @DisplayName("Lowercase 토큰 필터")
    @Test
    void analyze_with_lowercase_token_filter() throws Exception {
        //TODO Lowercase 토큰 필터를 테스트하기 위해 인덱스 생성
        CreateIndexRequest createIndexRequest = new CreateIndexRequest("movie_lower_analyzer");
        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();
        {
            builder.startObject("analysis");
            {
                builder.startObject("analyzer");
                {
                    builder.startObject("lowercase_analyzer");
                    {
                        builder.field("tokenizer", "standard");
                        builder.startArray("filter");
                        {
                            builder.value("lowercase");
                        }
                        builder.endArray();

                    }
                    builder.endObject();
                }
                builder.endObject();
            }
            builder.endObject();
        }
        builder.endObject();


        createIndexRequest.settings(builder);

        CreateIndexResponse createIndexResponse = testContainerClient.indices().create(createIndexRequest, RequestOptions.DEFAULT);
        assertCreatedIndex(createIndexResponse, "movie_lower_analyzer");

        //TODO Lowercase 토큰 필터 사용
        AnalyzeRequest analyzeRequest = AnalyzeRequest.withIndexAnalyzer("movie_lower_analyzer", "lowercase_analyzer", "Harry Potter and the Chamber of Secrets");
        AnalyzeResponse analyzeResponse = testContainerClient.indices().analyze(analyzeRequest, RequestOptions.DEFAULT);
        List<String> collect = analyzeResponse.getTokens()
                .stream()
                .map(AnalyzeResponse.AnalyzeToken::getTerm)
                .collect(Collectors.toList());

        System.out.println("collect = " + collect);
    }

    @DisplayName("Uppercase 토큰 필터")
    @Test
    void analyze_with_uppercase_token_filter() throws Exception {
        //TODO Uppercase 토큰 필터를 테스트하기 위해 인덱스 생성
        CreateIndexRequest createIndexRequest = new CreateIndexRequest("movie_upper_analyzer");
        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();
        {
            builder.startObject("analysis");
            {
                builder.startObject("analyzer");
                {
                    builder.startObject("uppercase_analyzer");
                    {
                        builder.field("tokenizer", "standard");
                        builder.startArray("filter");
                        {
                            builder.value("uppercase");
                        }
                        builder.endArray();
                    }
                    builder.endObject();
                }
                builder.endObject();
            }
            builder.endObject();
        }
        builder.endObject();


        createIndexRequest.settings(builder);

        CreateIndexResponse createIndexResponse = testContainerClient.indices().create(createIndexRequest, RequestOptions.DEFAULT);
        assertCreatedIndex(createIndexResponse, "movie_upper_analyzer");

        //TODO Uppercase 토큰 필터를 이용해 토큰을 변경한 예
        AnalyzeRequest analyzeRequest = AnalyzeRequest.withIndexAnalyzer("movie_upper_analyzer", "uppercase_analyzer", "Harry Potter and The Chamber of Secrets");
        AnalyzeResponse analyzeResponse = testContainerClient.indices().analyze(analyzeRequest, RequestOptions.DEFAULT);
        List<String> collect = analyzeResponse.getTokens()
                .stream()
                .map(AnalyzeResponse.AnalyzeToken::getTerm)
                .collect(Collectors.toList());

        System.out.println("collect = " + collect);
    }


    @DisplayName("Stop 토큰 필터")
    @Test
    void analyze_with_stop_token_filter() throws Exception {
        //TODO Stop 토큰 필터 테스트하기 위해 인덱스 생성
        CreateIndexRequest createIndexRequest = new CreateIndexRequest("movie_stop_analyzer");
        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();
        {
            builder.startObject("analysis");
            {
                builder.startObject("analyzer");
                {
                    builder.startObject("stop_filter_analyzer");
                    {
                        builder.field("tokenizer", "standard");
                        builder.startArray("filter");
                        {
                            builder.value("stop_filter");
                        }
                        builder.endArray();
                    }
                    builder.endObject();
                }
                builder.endObject();
                builder.startObject("filter");
                {
                    builder.startObject("stop_filter");
                    {
                        builder.field("type", "stop");
                        builder.startArray("stopwords");
                        {
                            builder.value("and")
                                    .value("is")
                                    .value("the");
                        }
                        builder.endArray();
                    }
                    builder.endObject();
                }
                builder.endObject();
            }
            builder.endObject();
        }
        builder.endObject();


        createIndexRequest.settings(builder);

        CreateIndexResponse createIndexResponse = testContainerClient.indices().create(createIndexRequest, RequestOptions.DEFAULT);
        assertCreatedIndex(createIndexResponse, "movie_stop_analyzer");

        //TODO Stop 토큰 필터를 이용해 토큰 변경
        AnalyzeRequest analyzeRequest = AnalyzeRequest.withIndexAnalyzer("movie_stop_analyzer", "stop_filter_analyzer", "Harry Potter and the Chamber of Secrets");
        AnalyzeResponse analyzeResponse = testContainerClient.indices().analyze(analyzeRequest, RequestOptions.DEFAULT);
        List<String> collect = analyzeResponse.getTokens()
                .stream()
                .map(AnalyzeResponse.AnalyzeToken::getTerm)
                .collect(Collectors.toList());

        System.out.println("collect = " + collect);
    }

    @DisplayName("Stemmer 토큰 필터")
    @Test
    void analyze_with_stemmer_token_filter() throws Exception {
        //TODO Stemmer 토큰 필터를 테스트하기 위한 인덱스 생성
        CreateIndexRequest createIndexRequest = new CreateIndexRequest("movie_stem_analyzer");
        XContentBuilder builder = XContentFactory.jsonBuilder();

        builder.startObject();
        {
            builder.startObject("analysis");
            {
                builder.startObject("analyzer");
                {
                    builder.startObject("stemmer_eng_analyzer");
                    {
                        builder.field("tokenizer", "standard");
                        builder.startArray("filter");
                        {
                            builder.value("lowercase")
                                    .value("stemmer_eng_filter");
                        }
                        builder.endArray();
                    }
                    builder.endObject();
                }
                builder.endObject();
                builder.startObject("filter");
                {
                    builder.startObject("stemmer_eng_filter");
                    {
                        builder.field("type", "stemmer")
                                .field("name", "english");
                    }
                    builder.endObject();
                }
                builder.endObject();
            }
            builder.endObject();
        }
        builder.endObject();

        createIndexRequest.settings(builder);

        CreateIndexResponse createIndexResponse = testContainerClient.indices().create(createIndexRequest, RequestOptions.DEFAULT);
        assertCreatedIndex(createIndexResponse, "movie_stem_analyzer");

        //TODO Stemmer 토큰 필터를 이용해 토큰 변경
        AnalyzeRequest analyzeRequest = AnalyzeRequest.withIndexAnalyzer("movie_stem_analyzer", "stemmer_eng_analyzer", "Herry Potter and the Chamber of Secrets");

        AnalyzeResponse analyzeResponse = testContainerClient.indices().analyze(analyzeRequest, RequestOptions.DEFAULT);
        List<String> collect = analyzeResponse.getTokens()
                .stream()
                .map(AnalyzeResponse.AnalyzeToken::getTerm)
                .collect(Collectors.toList());

        System.out.println("collect = " + collect);
    }


    @DisplayName("Synonym 토큰 필터")
    @Test
    void analyze_with_synonym_token_filter() throws Exception {
        //TODO Synonym 토큰 필터를 테스트하기 위한 인덱스 생성
        CreateIndexRequest createIndexRequest = new CreateIndexRequest("movie_syno_analyzer");
        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();
        {
            builder.startObject("analysis");
            {
                builder.startObject("analyzer");
                {
                    builder.startObject("synonym_analyzer");
                    {
                        builder.field("tokenizer", "whitespace");
                        builder.startArray("filter");
                        {
                            builder.value("synonym_filter");
                        }
                        builder.endArray();
                    }
                    builder.endObject();
                }
                builder.endObject();
                builder.startObject("filter");
                {
                    builder.startObject("synonym_filter");
                    {
                        builder.field("type", "synonym");
                        builder.startArray("synonyms");
                        {
                            builder.value("Harry => 해리");
                        }
                        builder.endArray();
                    }
                    builder.endObject();
                }
                builder.endObject();
            }
            builder.endObject();
        }
        builder.endObject();

        createIndexRequest.settings(builder);
        CreateIndexResponse createIndexResponse = testContainerClient.indices().create(createIndexRequest, RequestOptions.DEFAULT);
        assertCreatedIndex(createIndexResponse, "movie_syno_analyzer");

        //TODO Synonym 토큰 필터를 이용해 토큰을 변경
        AnalyzeRequest analyzeRequest = AnalyzeRequest.withIndexAnalyzer("movie_syno_analyzer", "synonym_analyzer", "Harry Potter and the Chamber of Secrets");
        AnalyzeResponse analyzeResponse = testContainerClient.indices().analyze(analyzeRequest, RequestOptions.DEFAULT);
        List<String> collect = analyzeResponse.getTokens()
                .stream()
                .map(AnalyzeResponse.AnalyzeToken::getTerm)
                .collect(Collectors.toList());

        System.out.println("collect = " + collect);
    }


    @DisplayName("Trim 토큰 필터")
    @Test
    void analyze_by_trim_token_filter() throws Exception {
        //TODO Trim 토큰 필터 테스트를 위한 인덱스 생성
        CreateIndexRequest createIndexRequest = new CreateIndexRequest("movie_trim_analyzer");
        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();
        {
            builder.startObject("analysis");
            {
                builder.startObject("analyzer");
                {
                    builder.startObject("trim_analyzer");
                    {
                        builder.field("tokenizer", "keyword");
                        builder.startArray("filter");
                        {
                            builder.value("lowercase")
                                    .value("trim");
                        }
                        builder.endArray();
                    }
                    builder.endObject();
                }
                builder.endObject();
            }
            builder.endObject();
        }
        builder.endObject();


        createIndexRequest.settings(builder);

        CreateIndexResponse createIndexResponse = testContainerClient.indices().create(createIndexRequest, RequestOptions.DEFAULT);
        assertCreatedIndex(createIndexResponse, "movie_trim_analyzer");
        //TODO Trim 토큰을 이용해 토큰을 변경
        AnalyzeRequest analyzeRequest = AnalyzeRequest.withIndexAnalyzer("movie_trim_analyzer", "trim_analyzer", "          Harry Potter and the Chamber of Secrets         ");
        AnalyzeResponse analyzeResponse = testContainerClient.indices().analyze(analyzeRequest, RequestOptions.DEFAULT);
        List<String> collect = analyzeResponse.getTokens()
                .stream()
                .map(AnalyzeResponse.AnalyzeToken::getTerm)
                .collect(Collectors.toList());

        System.out.println("collect = " + collect);
    }

    @DisplayName("인덱스 생성 시 매핑 정보에 동의어 사전을 등록하는 설정")
    @Test
    void index_with_synonym() throws Exception {
        //TODO 동의어 매핑을 위한 인덱스 생성
        CreateIndexRequest createIndexRequest = new CreateIndexRequest("movie_analyzer");
        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();
        {
            builder.startObject("analysis");
            {
                builder.startObject("analyzer");
                {
                    builder.startObject("synonym_analyzer");
                    {
                        builder.field("tokenizer", "standard");
                        builder.startArray("filter");
                        {
                            builder.value("lowercase")
                                    .value("synonym_filter");
                        }
                        builder.endArray();
                    }
                    builder.endObject();
                }
                builder.endObject();
                builder.startObject("filter");
                {
                    builder.startObject("synonym_filter");
                    {
                        builder.field("type", "synonym")
                                .field("ignore_case", "true")
                                .field("synonyms_path", "analysis/synonym.txt");
                    }
                    builder.endObject();
                }
                builder.endObject();
            }
            builder.endObject();
        }
        builder.endObject();


        createIndexRequest.settings(builder);

        CreateIndexResponse createIndexResponse = testContainerClient.indices().create(createIndexRequest, RequestOptions.DEFAULT);
        assertCreatedIndex(createIndexResponse, "movie_analyzer");


        //TODO 동의어 사전이 잘 작동하는지 테스트
        AnalyzeRequest analyzeRequest = AnalyzeRequest.withIndexAnalyzer("movie_analyzer", "synonym_analyzer", "Elasticsearch Harry Potter");
        AnalyzeResponse analyzeResponse = testContainerClient.indices().analyze(analyzeRequest, RequestOptions.DEFAULT);
        List<String> collect = analyzeResponse.getTokens()
                .stream()
                .map(AnalyzeResponse.AnalyzeToken::getTerm)
                .collect(Collectors.toList());
        System.out.println("collect = " + collect);
    }


    @DisplayName("Delete By Query 기능")
    @Test
    void delete_by_query() throws Exception {
        IndexRequest indexRequest = new IndexRequest("movie_dynamic");
        Map<String, Object> source = new HashMap<>();
        source.put("movieCd", "20173732");
        source.put("movieNm", "살아남은 아이");
        source.put("movieNmEn", "Last Child");

        indexRequest.source(source);

        IndexResponse indexResponse = testContainerClient.index(indexRequest, RequestOptions.DEFAULT);
        System.out.println("indexResponse = " + indexResponse);

        Thread.sleep(2000);


        DeleteByQueryRequest deleteByQueryRequest = new DeleteByQueryRequest("movie_dynamic");
        deleteByQueryRequest.setQuery(
                termQuery("movieCd", "20173732")
        );

        BulkByScrollResponse bulkByScrollResponse = testContainerClient.deleteByQuery(deleteByQueryRequest, RequestOptions.DEFAULT);
        System.out.println("bulkByScrollResponse = " + bulkByScrollResponse);
    }


    @DisplayName("Update API")
    @Test
    void update_api() throws Exception {
        IndexRequest indexRequest = new IndexRequest("movie_dynamic");
        Map<String, Object> source = new HashMap<>();
        source.put("counter", 1000);
        source.put("movieNmEn", "Last Child");

        indexRequest.id("1")
                .source(source);
        IndexResponse indexResponse = testContainerClient.index(indexRequest, RequestOptions.DEFAULT);
        assertEquals("1", indexResponse.getId());
        Thread.sleep(2000);


        UpdateRequest updateRequest = new UpdateRequest("movie_dynamic", "1");
        Map<String, Object> params = new HashMap<>();
        params.put("count", 1);
        Script script = new Script(ScriptType.INLINE, "painless", "ctx._source.counter += params.count", params);

        updateRequest.script(script);

        UpdateResponse updateResponse = testContainerClient.update(updateRequest, RequestOptions.DEFAULT);
        System.out.println("updateResponse = " + updateResponse);


        GetRequest getRequest = new GetRequest("movie_dynamic", "1");
        GetResponse getResponse = testContainerClient.get(getRequest, RequestOptions.DEFAULT);
        assertEquals(1001, getResponse.getSourceAsMap().get("counter"));
    }


    @DisplayName("Bulk API")
    @Test
    void bulk_api() throws Exception {
        BulkRequest bulkRequest = new BulkRequest();
        AtomicInteger i = new AtomicInteger(1);
        List.of("살아남은 아이", "해리포터와 비밀의 방", "어벤저스")
                .forEach(e -> {
                    IndexRequest indexRequest = new IndexRequest("movie_dynamic")
                            .id(String.valueOf(i.getAndIncrement()))
                            .source(Map.of("title", e));
                    bulkRequest.add(indexRequest);
                });


        BulkResponse bulk = testContainerClient.bulk(bulkRequest, RequestOptions.DEFAULT);
        boolean result = Arrays.stream(bulk.getItems())
                .allMatch(e -> e.getOpType().equals(DocWriteRequest.OpType.INDEX));
        assertTrue(result);

    }




    private List<Map<String, Object>> readValue(InputStream inputStream) {
        List<Map<String, Object>> result = null;
        try {
            result = new ObjectMapper().readValue(inputStream, new TypeReference<>() {
            });
        } catch (IOException e) {
            e.printStackTrace();
        }
        return result;
    }


    private InputStream getInputStream(Resource resource) {
        InputStream inputStream = null;
        try {
            inputStream = resource.getInputStream();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return inputStream;
    }


    private XContentBuilder createMappingBuilder() throws IOException {
        XContentBuilder mappingBuilder = XContentFactory.jsonBuilder();
        mappingBuilder.startObject();
        {
            mappingBuilder.startObject("properties");
            {
                defineField(mappingBuilder, "movieCd", "keyword");
                defineFieldWithAnalyzer(mappingBuilder, "movieNm", "text");
                defineFieldWithAnalyzer(mappingBuilder, "movieNmEn", "text");
                defineField(mappingBuilder, "prdtYear", "integer");
                defineField(mappingBuilder, "openDt", "integer");
                defineField(mappingBuilder, "typeNm", "keyword");
                defineField(mappingBuilder, "prdtStatNm", "keyword");
                defineField(mappingBuilder, "nationAlt", "keyword");
                defineField(mappingBuilder, "genreAlt", "keyword");
                defineField(mappingBuilder, "repNationNm", "keyword");
                defineField(mappingBuilder, "repGenreNm", "keyword");

                mappingBuilder.startObject("companies");
                {
                    mappingBuilder.startObject("properties");
                    {
                        mappingBuilder.startObject("companyCd");
                        {
                            mappingBuilder.field("type", "keyword");
                        }
                        mappingBuilder.endObject();
                        mappingBuilder.startObject("companyNm");
                        {
                            mappingBuilder.field("type", "keyword");
                        }
                        mappingBuilder.endObject();
                    }
                    mappingBuilder.endObject();
                }
                mappingBuilder.endObject();


                mappingBuilder.startObject("directors");
                {
                    mappingBuilder.startObject("properties");
                    {
                        mappingBuilder.startObject("peopleNm");
                        {
                            mappingBuilder.field("type", "keyword");
                        }
                        mappingBuilder.endObject();
                    }
                    mappingBuilder.endObject();
                }
                mappingBuilder.endObject();

            }
            mappingBuilder.endObject();
        }
        mappingBuilder.endObject();
        return mappingBuilder;
    }

    private void defineField(XContentBuilder mappingBuilder, String field, String type) throws IOException {
        mappingBuilder.startObject(field);
        {
            mappingBuilder.field("type", type);
        }
        mappingBuilder.endObject();
    }

    private void defineFieldWithAnalyzer(XContentBuilder mappingBuilder, String field, String type) throws IOException {
        mappingBuilder.startObject(field);
        {
            mappingBuilder.field("type", type);
            mappingBuilder.field("analyzer", "standard");
        }
        mappingBuilder.endObject();
    }

    private XContentBuilder createSettingBuilder() throws IOException {
        XContentBuilder settingBuilder = XContentFactory.jsonBuilder();
        settingBuilder.startObject();
        {
            settingBuilder.field(NUMBER_OF_SHARDS, 5);
            settingBuilder.field(NUMBER_OF_REPLICAS, 1);
        }
        settingBuilder.endObject();
        return settingBuilder;
    }
}
