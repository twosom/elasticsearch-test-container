package com.gravylab.elasticsearchguide;

import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.client.indices.PutMappingRequest;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.suggest.SuggestBuilder;
import org.elasticsearch.search.suggest.SuggestBuilders;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.text.Normalizer;
import java.util.Arrays;
import java.util.Map;

import static org.elasticsearch.index.query.QueryBuilders.*;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 반드시 ElasticSearch 노드에 javacafe plugin 이 설치되어 있어야 한다.
 */
public class KoreanPluginTest extends CommonTestClass {


    public static final String COMPANY_SPELLCHECKER = "company_spellchecker";
    public static final String KOREAN_SPELL_ANALYZER = "korean_spell_analyzer";
    public static final String COMPANY = "company";
    public static final String MY_ANALYZER = "my_analyzer";
    public static final String SEARCH_KEYWORD = "search_keyword";
    public static final String KOR_2_ENG_ANALYZER = "kor2eng_analyzer";
    public static final String ENG_2_KOR_ANALYZER = "eng2kor_analyzer";
    public static final String STANDARD = "standard";
    public static final String CUSTOM = "custom";
    public static final String TRIM = "trim";
    public static final String LOWERCASE = "lowercase";
    public static final String AC_TEST = "ac_test";
    public static final String NGRAM_ANALYZER = "ngram_analyzer";
    public static final String EDGE_NGRAM_ANALYZER = "edge_ngram_analyzer";
    public static final String EDGE_NGRAM_ANALYZER_BACK = "edge_ngram_analyzer_back";
    public static final String AC_TEST_2 = "ac_test2";
    public static final String AC_TEST_3 = "ac_test_3";
    public static final String AC_TEST_4 = "ac_test_4";
    public static final String KEYWORD = "keyword";
    public static final String LETTER = "letter";
    public static final String DIGIT = "digit";
    public static final String PUNCTUATION = "punctuation";
    public static final String SYMBOL = "symbol";
    public static final String MAX_NGRAM_DIFF = "max_ngram_diff";

    @DisplayName("한글 분석을 위한 인덱스 생성")
    @Test
    void create_index_for_korean_analyzer() throws Exception {

        removeIndexIfExists(COMPANY_SPELLCHECKER, dockerClient);

        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();
        {
            builder.startObject("analysis");
            {
                builder.startObject("analyzer");
                {
                    builder.startObject(KOREAN_SPELL_ANALYZER);
                    {
                        builder.field("type", CUSTOM);
                        builder.field("tokenizer", STANDARD);
                        builder.startArray("filter");
                        {
                            builder.value(TRIM)
                                    .value(LOWERCASE)
                                    .value("javacafe_spell");
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
        CreateIndexRequest createIndexRequest = new CreateIndexRequest(COMPANY_SPELLCHECKER)
                .settings(builder);


        dockerClient.indices().create(createIndexRequest, RequestOptions.DEFAULT);
    }

    @DisplayName("자동완성을 적용할 매핑 설정")
    @Test
    void mapping_for_korean_analyzer() throws Exception {
        create_index_for_korean_analyzer();
        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();
        {
            builder.startObject("properties");
            {
                builder.startObject("name");
                {
                    builder.field("type", KEYWORD);
                    builder.startArray("copy_to");
                    {
                        builder.value("suggest");
                    }
                    builder.endArray();
                }
                builder.endObject();


                builder.startObject("suggest");
                {
                    builder.field("type", "completion");
                    builder.field("analyzer", KOREAN_SPELL_ANALYZER);
                }
                builder.endObject();
            }
            builder.endObject();
        }
        builder.endObject();


        PutMappingRequest putMappingRequest = new PutMappingRequest(COMPANY_SPELLCHECKER)
                .source(builder);

        AcknowledgedResponse putMappingResponse = dockerClient.indices().putMapping(putMappingRequest, RequestOptions.DEFAULT);
        assertTrue(putMappingResponse.isAcknowledged());
    }


    @DisplayName("오타 교정용 데이터 색인")
    @Test
    void index_for_term_suggest() throws Exception {
        mapping_for_korean_analyzer();

        IndexRequest indexRequest = new IndexRequest(COMPANY_SPELLCHECKER)
                .source(Map.of("name", "삼성전자"));


        dockerClient.index(indexRequest, RequestOptions.DEFAULT);
        Thread.sleep(1500);
    }

    @DisplayName("오타 교정 API 요청")
    @Test
    void request_for_term_suggest() throws Exception {
        index_for_term_suggest();
        SearchRequest searchRequest = new SearchRequest(COMPANY_SPELLCHECKER)
                .source(
                        new SearchSourceBuilder()
                                .suggest(
                                        new SuggestBuilder()
                                                .addSuggestion("my-suggest",
                                                        SuggestBuilders.termSuggestion("suggest")
                                                                .text("샴성전자")
                                                )
                                )
                );


        SearchResponse searchResponse = dockerClient.search(searchRequest, RequestOptions.DEFAULT);
        String keyword = searchResponse.getSuggest()
                .getSuggestion("my-suggest")
                .getEntries()
                .get(0)
                .getOptions()
                .get(0)
                .getText()
                .toString();


        String normalize = Normalizer.normalize(keyword, Normalizer.Form.NFC);
        System.out.println("normalize = " + normalize);
    }

    //TODO 한영/영한 오타 교정
    @DisplayName("한영/영한 오타교정을 위한 인덱스 생성")
    @Test
    void create_index_for_korean_term_suggest() throws Exception {
        removeIndexIfExists(COMPANY, dockerClient);
        removeIndexIfExists(SEARCH_KEYWORD, dockerClient);


        XContentBuilder builder = XContentFactory.jsonBuilder();

        builder.startObject();
        {
            builder.startObject("analysis");
            {
                builder.startObject("analyzer");
                {
                    builder.startObject("my_analyzer");
                    {
                        builder.field("type", CUSTOM);
                        builder.field("tokenizer", STANDARD);
                        builder.startArray("filter");
                        {
                            builder.value(TRIM)
                                    .value(LOWERCASE);
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


        CreateIndexRequest createIndexRequest = new CreateIndexRequest(COMPANY)
                .settings(builder);
        dockerClient.indices().create(createIndexRequest, RequestOptions.DEFAULT);


        builder = XContentFactory.jsonBuilder();

        builder.startObject();
        {
            builder.startObject("analysis");
            {
                builder.startObject("analyzer");
                {

                    builder.startObject(KOR_2_ENG_ANALYZER);
                    {
                        builder.field("type", CUSTOM);
                        builder.field("tokenizer", STANDARD);
                        builder.startArray("filter");
                        {
                            builder.value(TRIM)
                                    .value(LOWERCASE)
                                    .value("javacafe_kor2eng");
                        }
                        builder.endArray();

                    }
                    builder.endObject();


                    builder.startObject(ENG_2_KOR_ANALYZER);
                    {
                        builder.field("type", CUSTOM);
                        builder.field("tokenizer", STANDARD);
                        builder.startArray("filter");
                        {
                            builder.value(TRIM)
                                    .value(LOWERCASE)
                                    .value("javacafe_eng2kor");
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

        createIndexRequest = new CreateIndexRequest(SEARCH_KEYWORD)
                .settings(builder);


        dockerClient.indices().create(createIndexRequest, RequestOptions.DEFAULT);
    }

    @DisplayName("매핑 설정")
    @Test
    void mapping_for_created_indices() throws Exception {
        create_index_for_korean_term_suggest();
        XContentBuilder builder = XContentFactory.jsonBuilder();

        builder.startObject();
        {
            builder.startObject("properties");
            {
                builder.startObject("name");
                {
                    builder.field("type", KEYWORD);
                }
                builder.endObject();
            }
            builder.endObject();
        }
        builder.endObject();


        PutMappingRequest putMappingRequest = new PutMappingRequest(COMPANY)
                .source(builder);


        AcknowledgedResponse putMappingResponse = dockerClient.indices().putMapping(putMappingRequest, RequestOptions.DEFAULT);
        assertTrue(putMappingResponse.isAcknowledged());


        builder = XContentFactory.jsonBuilder();
        builder.startObject();
        {
            builder.startObject("properties");
            {
                builder.startObject("name");
                {
                    builder.field("type", KEYWORD);
                    builder.startArray("copy_to");
                    {
                        builder.value("kor2eng_suggest")
                                .value("eng2kor_suggest");
                    }
                    builder.endArray();
                }
                builder.endObject();


                builder.startObject("kor2eng_suggest");
                {
                    builder.field("type", "text");
                    //TODO 인덱싱 시에는 기존 작업 그대로
                    builder.field("analyzer", STANDARD);
                    //TODO 검색 시에는 kor2eng 분석기 사용
                    builder.field("search_analyzer", KOR_2_ENG_ANALYZER);
                }
                builder.endObject();


                builder.startObject("eng2kor_suggest");
                {
                    builder.field("type", "text");
                    builder.field("analyzer", STANDARD);
                    builder.field("search_analyzer", ENG_2_KOR_ANALYZER);
                }
                builder.endObject();
            }
            builder.endObject();
        }
        builder.endObject();


        putMappingRequest = new PutMappingRequest(SEARCH_KEYWORD)
                .source(builder);


        putMappingResponse = dockerClient.indices().putMapping(putMappingRequest, RequestOptions.DEFAULT);
        assertTrue(putMappingResponse.isAcknowledged());
    }

    @DisplayName("오타 교정용 데이터 색인")
    @Test
    void index_data_for_korean_term_suggest() throws Exception {
        mapping_for_created_indices();

        //TODO 데이터 인덱스와 오타 교정 인덱스에 동일한 데이터를 색인.
        // 한영 오타 교정과 영한 오타 교정 테스트를 위해 가각 2건의 데이터를 색인했다.
        BulkRequest bulkRequest = new BulkRequest();
        bulkRequest.add(
                        new IndexRequest()
                                .index(COMPANY)
                                .id("1")
                                .source(Map.of("name", "삼성전자"))
                )
                .add(
                        new IndexRequest()
                                .index(COMPANY)
                                .id("2")
                                .source(Map.of("name", "iphone"))
                )
                .add(
                        new IndexRequest()
                                .index(SEARCH_KEYWORD)
                                .id("1")
                                .source(Map.of("name", "삼성전자"))
                )
                .add(
                        new IndexRequest()
                                .index(SEARCH_KEYWORD)
                                .id("2")
                                .source(Map.of("name", "iphone"))
                );

        BulkResponse bulkResponse = dockerClient.bulk(bulkRequest, RequestOptions.DEFAULT);
        assertBulkIndexResponse(bulkResponse);
        Thread.sleep(2000);
    }

    @DisplayName("오타 교정 API 요창")
    @Test
    void request_for_korean_term_suggest() throws Exception {
        index_data_for_korean_term_suggest();
        //TODO 오타 교정은 보통 당므과 같이 동작한다.
        // 먼저 오타 교정 API 를 실행하고 결과에 따라 실제 인덱스를 검색하기 위한 검색어를 결정한다.
        // 1. search_keyword 인덱스에 사용자 검색어를 가지고 검색 질의를 한다.
        // 2. 검색 결과가 없다면 검색어 그대로 company 인덱스에 검색 질의를 한다.
        // 3. 검색 결과가 있다면 변경된 검색어로 company 인덱스에 검색 질의를 한다.

        //TODO 먼저 한영 오타에 대한 테스트 진행
        SearchRequest searchRequest = new SearchRequest(SEARCH_KEYWORD)
                .source(
                        new SearchSourceBuilder()
                                .query(
                                        matchQuery("eng2kor_suggest", "tkatjdwjswk")
                                                .queryName("query")
                                )
                );

        printSearchResponse(searchRequest);


        //TODO 영한 오타 테스트.
        // 사용자는 "iphone" 을 검색하고 싶었지만 키보드의 한영키가 한글로 설정돼 있어 실제로는 "ㅑㅔㅗㅐㅜㄷ" 이 요청됐다.
        // 한영 오타와 마찬가지로 search_keyword 인덱스를 검색한다.

        searchRequest = new SearchRequest(SEARCH_KEYWORD)
                .source(
                        new SearchSourceBuilder()
                                .query(
                                        matchQuery("kor2eng_suggest", "ㅑㅔㅗㅐㅜㄷ")
                                                .queryName("query")
                                )
                );


        printSearchResponse(searchRequest);

        //TODO 한영 오타나 영한 오타가 발생하더라도 실제 데이터를 질의할 때 변경된 검색어로 질의했기 때문에 정상적인 문서가 검색된다.
        // 이를 가능케 하는 것이 search_keyword 인덱스다. 하지만 search_keyword 인덱스에 키워드 자체가 존재하지 않는다면
        // 오타 교정이 불가능하기 때문에 검색어에 대한 모니터링을 수시로 수행해야 한다.
        // 검색 질의 시 로그 등을 로그스태시에 저장하고 검색어와 검색 겨로가가 0건인 경우를 늘 모니터링해야 검색 품질을 높일 수 있다.
    }

    private void printSearchResponse(SearchRequest searchRequest) throws IOException {
        SearchResponse searchResponse = dockerClient.search(searchRequest, RequestOptions.DEFAULT);
        Arrays.stream(searchResponse
                        .getInternalResponse()
                        .hits()
                        .getHits())
                .map(SearchHit::getSourceAsMap)
                .forEach(System.err::println);
    }


    //TODO Completion Suggest API 를 이용한 한글 자동완성
    @DisplayName("인덱스 생성")
    @Test
    void create_index_for_korean_completion_suggest() throws Exception {
        removeIndexIfExists(AC_TEST, dockerClient);

        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();
        {
            builder.field(NUMBER_OF_SHARDS, 5);
            builder.field(NUMBER_OF_REPLICAS, 1);
        }
        builder.endObject();

        CreateIndexRequest createIndexRequest = new CreateIndexRequest(AC_TEST)
                .settings(builder);
        dockerClient.indices().create(createIndexRequest, RequestOptions.DEFAULT);
    }


    @DisplayName("매핑 설정")
    @Test
    void mapping_for_korean_completion_suggest() throws Exception {
        create_index_for_korean_completion_suggest();


        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();
        {
            builder.startObject("properties");
            {
                builder.startObject("itemSrc");
                {
                    builder.field("type", KEYWORD);
                }
                builder.endObject();

                builder.startObject("itemCompletion");
                {
                    builder.field("type", "completion");
                }
                builder.endObject();

            }
            builder.endObject();
        }
        builder.endObject();


        PutMappingRequest putMappingRequest = new PutMappingRequest(AC_TEST)
                .source(builder);
        AcknowledgedResponse putMappingResponse = dockerClient.indices().putMapping(putMappingRequest, RequestOptions.DEFAULT);
        assertTrue(putMappingResponse.isAcknowledged());
    }

    @DisplayName("자동완성 데이터 색인")
    @Test
    void index_data_for_korean_auto_completion() throws Exception {

        mapping_for_korean_completion_suggest();

        BulkRequest bulkRequest = new BulkRequest()
                .add(
                        new IndexRequest()
                                .index(AC_TEST)
                                .id("1")
                                .source(Map.of("itemSrc", "신혼", "itemCompletion", "신혼"))
                )
                .add(
                        new IndexRequest()
                                .index(AC_TEST)
                                .id("2")
                                .source(Map.of("itemSrc", "신혼가전", "itemCompletion", "신혼가전"))
                )
                .add(
                        new IndexRequest()
                                .index(AC_TEST)
                                .id("3")
                                .source(Map.of("itemSrc", "신혼가전특별전", "itemCompletion", "신혼가전특별전"))
                );

        BulkResponse bulkResponse = dockerClient.bulk(bulkRequest, RequestOptions.DEFAULT);
        assertBulkIndexResponse(bulkResponse);
        Thread.sleep(1500);
    }

    @DisplayName("suggest API 를 이용하여 자동완성 검색")
    @Test
    void search_with_suggest_api() throws Exception {
        index_data_for_korean_auto_completion();
        SearchRequest searchRequest = new SearchRequest(AC_TEST)
                .source(
                        new SearchSourceBuilder()
                                .suggest(
                                        new SuggestBuilder()
                                                .addSuggestion("s1",
                                                        SuggestBuilders.completionSuggestion("itemCompletion")
                                                                .prefix("신혼")
                                                                .size(10)
                                                )

                                )
                );
        SearchResponse searchResponse = dockerClient.search(searchRequest, RequestOptions.DEFAULT);

        searchResponse.getSuggest()
                .getSuggestion("s1")
                .getEntries()
                .get(0)
                .getOptions()
                .forEach(System.err::println);
    }


    //TODO Ngram 분석기
    // 음절 단위로 토큰을 생성하기 때문에 재현율은 높으나 정확도는 떨어진다.
    // 첫 음절을 기준으로 max_gram 에서 지정한 길이만큼 토큰을 생성한다.


    //TODO 원문 : 아버지가 방에 들어가신다.
    // 1단계 분석 : [아버지가, 방에, 들어가신다]
    // 2단계 분석 : [아, 아버, 아버지, 아버지가, 버, 버지, 버지가, 지, 지가, 자]
    //            [방, 방에, 에]
    //            [들, 들어, 들어가, 들어가신, 들어가신다, 어, 어가, 어가신, 어가신다, 가, 가신, 가신다, 신, 신다, 다]
    // Edge Ngram 분석기
    //  대부분 Ngram 과 유사하게 동작한다.
    //  지정한 토크나이저의 특서에 따라 Ngram 이 일어난다.
    // 원문 : 아버지가 방에 들어가신다
    // 1단계 분석 : [아버지가, 방에, 들어가신다]
    // 2단계 분석 : [아, 아버, 아버지, 아버지가]
    //            [방, 방에]
    //            [들, 들어, 들어가, 들어가신, 들어가신다]
    // Edge Ngram Back Analyzer
    //  Edge Ngram 과 반대로 동작하는 토크나이저를 사용한다.
    //  옵션으로 "side:back" 을 반드시 설정해야 한다.
    // 원문 : 아버지가 방에 들어가신다
    //  1단계 분석 : [아버지가, 방에, 들어가신다]
    //  2단계 분석 : [아, 버, 아버, 지, 버지, 아버지, 가, 지가, 버지가, 아버지가]
    //             [방, 에, 방에]
    //             [들, 어, 들어, 가, 어가, 들어가, 신, 가신, 어가신, 들어가신, 다, 신다, 어가신다, 들어가신다]


    //TODO Ngram 이 글자 단위로 토큰을 생성하기 때문에 Ngram 분석기, Edge Ngram 분석기, Edge Ngram Back 분석기라는 총 세 가지 분석기를 모두 사용하면 어떠한 부분일치라도 구현할 수 있다.
    // 자동완성을 위해 인덱스를 생성할 때 매핑 설정을 통해 해당 분석기를 정의하고, 필드에서 사용하도록 설정하면 된다.
    // 엘라스틱서치에서는 다음과 같이 자신만의 분석기를 정의하고 사용할 수 있다.

    @DisplayName("한글 자동완성을 위한 토크나이저 정의")
    @Test
    void create_ngram_tokenizer_for_korean_autocomplete() throws Exception {

        removeIndexIfExists(AC_TEST_2, dockerClient);
        //TODO 인덱스 생성
        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();
        {
            builder.field(NUMBER_OF_SHARDS, 5);
            builder.field(NUMBER_OF_REPLICAS, 1);
            builder.field(MAX_NGRAM_DIFF, 50);

            builder.startObject("analysis");
            {
                builder.startObject("analyzer");
                {
                    builder.startObject(NGRAM_ANALYZER);
                    {
                        builder.field("type", CUSTOM);
                        builder.field("tokenizer", "ngram_tokenizer");
                        builder.startArray("filter");
                        {
                            builder.value(LOWERCASE)
                                    .value(TRIM);
                        }
                        builder.endArray();
                    }
                    builder.endObject();


                    builder.startObject(EDGE_NGRAM_ANALYZER);
                    {
                        builder.field("type", CUSTOM);
                        builder.field("tokenizer", "edge_ngram_tokenizer");
                        builder.startArray("filter");
                        {
                            builder.value(LOWERCASE)
                                    .value(TRIM)
                                    .value("edge_ngram_filter_front");
                        }
                        builder.endArray();
                    }
                    builder.endObject();

                    builder.startObject(EDGE_NGRAM_ANALYZER_BACK);
                    {
                        builder.field("type", CUSTOM);
                        builder.field("tokenizer", "edge_ngram_tokenizer");
                        builder.startArray("filter");
                        {
                            builder.value(LOWERCASE)
                                    .value(TRIM)
                                    .value("edge_ngram_filter_back");
                        }
                        builder.endArray();
                    }
                    builder.endObject();

                }
                builder.endObject();


                builder.startObject("tokenizer");
                {
                    builder.startObject("ngram_tokenizer");
                    {
                        builder.field("type", "ngram");
                        builder.field("min_gram", 1);
                        builder.field("max_gram", 50);
                        builder.startArray("token_chars");
                        {
                            builder.value(LETTER)
                                    .value(DIGIT)
                                    .value(PUNCTUATION)
                                    .value(SYMBOL);
                        }
                        builder.endArray();
                    }
                    builder.endObject();


                    builder.startObject("edge_ngram_tokenizer");
                    {
                        builder.field("type", "edge_ngram");
                        builder.field("min_gram", 1);
                        builder.field("max_gram", 50);
                        builder.startArray("token_chars");
                        {
                            builder.value(LETTER)
                                    .value(DIGIT)
                                    .value(PUNCTUATION)
                                    .value(SYMBOL);
                        }
                        builder.endArray();
                    }
                    builder.endObject();
                }
                builder.endObject();


                builder.startObject("filter");
                {
                    builder.startObject("edge_ngram_filter_front");
                    {
                        builder.field("type", "edge_ngram");
                        builder.field("min_gram", 1);
                        builder.field("max_gram", 50);
                        builder.field("side", "front");
                    }
                    builder.endObject();

                    builder.startObject("edge_ngram_filter_back");
                    {
                        builder.field("type", "edge_ngram");
                        builder.field("min_gram", 1);
                        builder.field("max_gram", 50);
                        builder.field("side", "back");
                    }
                    builder.endObject();
                }
                builder.endObject();
            }
            builder.endObject();
        }
        builder.endObject();
        CreateIndexRequest createIndexRequest = new CreateIndexRequest(AC_TEST_2)
                .settings(builder);

        dockerClient.indices().create(createIndexRequest, RequestOptions.DEFAULT);
    }

    @DisplayName("매핑 설정")
    @Test
    void mapping_for_korean_autocomplete() throws Exception {
        create_ngram_tokenizer_for_korean_autocomplete();
        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();
        {
            builder.startObject("properties");
            {

                //TODO 일반적인 매칭 검색 용도로 사용하는 필드다. 필드를 정의할 때 keyword 타입으로 설정한다.
                builder.startObject("item");
                {
                    builder.field("type", KEYWORD);
                    builder.field("boost", 30);
                }
                builder.endObject();

                //TODO Ngram 으로 분석된 정보를 자동완성으로 매칭하기 위한 필드다. 필드를 정의할 때 ngram_analyzer 를 분석기로 사용한다.
                builder.startObject("itemNgram");
                {
                    builder.field("type", "text");
                    builder.field("analyzer", NGRAM_ANALYZER);
                    builder.field("search_analyzer", NGRAM_ANALYZER);
                    builder.field("boost", 3);
                }
                builder.endObject();

                //TODO Edge Ngram 으로 분석된 정보를 자동완서응로 매칭하기 위한 필드다. 필드를 정의할 때 index_analyzer 와 search_analyzer 를 각각 다르게 등록한다.
                // 색인할 때는 edge_ngram_analyzer 를 분석기로 사용하고, 검색할 때는 ngram_analyzer 를 분석기로 사용한다.
                builder.startObject("itemNgramEdge");
                {
                    builder.field("type", "text");
                    builder.field("analyzer", EDGE_NGRAM_ANALYZER);
                    builder.field("search_analyzer", NGRAM_ANALYZER);
                    builder.field("boost", 2);
                }
                builder.endObject();

                //TODO Edge Ngram Back 으로 분석된 정보를 자동완성으로 매칭하기 위한 필드다. 필드를 정의할 때 index_analyzer 와 search_analyzer 를 각각 다르게 등록한다.
                // 색인할 때는 edge_ngram_analyzer_back 분석기로 사용하고 검색할 때는 ngram_analyzer 를 분석기로 사용한다.
                builder.startObject("itemNgramEdgeBack");
                {
                    builder.field("type", "text");
                    builder.field("analyzer", EDGE_NGRAM_ANALYZER_BACK);
                    builder.field("search_analyzer", NGRAM_ANALYZER);
                    builder.field("boost", 1);
                }
                builder.endObject();
            }
            builder.endObject();
        }
        builder.endObject();

        PutMappingRequest putMappingRequest = new PutMappingRequest(AC_TEST_2)
                .source(builder);

        AcknowledgedResponse putMappingResponse = dockerClient.indices().putMapping(putMappingRequest, RequestOptions.DEFAULT);
        assertTrue(putMappingResponse.isAcknowledged());
    }


    @DisplayName("자동완성 데이터 색인")
    @Test
    void index_data_for_korean_autocomplete() throws Exception {
        mapping_for_korean_autocomplete();
        BulkRequest bulkRequest = new BulkRequest()
                .add(
                        new IndexRequest()
                                .index(AC_TEST_2)
                                .id("1")
                                .source(Map.of(
                                        "item", "신혼",
                                        "itemNgram", "신혼",
                                        "itemNgramEdge", "신혼",
                                        "itemNgramEdgeBack", "신혼"))
                )
                .add(
                        new IndexRequest()
                                .index(AC_TEST_2)
                                .id("2")
                                .source(Map.of(
                                        "item", "신혼가전",
                                        "itemNgram", "신혼가전",
                                        "itemNgramEdge", "신혼가전",
                                        "itemNgramEdgeBack", "신혼가전"))
                )
                .add(
                        new IndexRequest()
                                .index(AC_TEST_2)
                                .id("3")
                                .source(Map.of(
                                        "item", "신혼가전특별전",
                                        "itemNgram", "신혼가전특별전",
                                        "itemNgramEdge", "신혼가전특별전",
                                        "itemNgramEdgeBack", "신혼가전특별전"))
                );

        BulkResponse bulkResponse = dockerClient.bulk(bulkRequest, RequestOptions.DEFAULT);
        assertBulkIndexResponse(bulkResponse);
        Thread.sleep(1500);
    }

    private void assertBulkIndexResponse(BulkResponse bulkResponse) {
        boolean result = Arrays.stream(bulkResponse.getItems())
                .map(BulkItemResponse::getOpType)
                .allMatch(e -> e.equals(DocWriteRequest.OpType.INDEX));
        assertTrue(result);
    }

    @DisplayName("한글 자동완성 테스트")
    @ParameterizedTest(name = "키워드 : {0}")
    @ValueSource(strings = {"신혼", "가전"})
    void search_test_for_korean_autocomplete(String keyword) throws Exception {
        index_data_for_korean_autocomplete();

        SearchRequest searchRequest = new SearchRequest(AC_TEST_2)
                .source(
                        new SearchSourceBuilder()
                                .query(
                                        boolQuery()
                                                .should(
                                                        prefixQuery("item", keyword)
                                                )
                                                .should(
                                                        termQuery("item", keyword)
                                                )
                                                .should(
                                                        termQuery("itemNgram", keyword)
                                                )
                                                .should(
                                                        termQuery("itemNgramEdge", keyword)
                                                )
                                                .should(
                                                        termQuery("itemNgramEdgeBack", keyword)
                                                )
                                                .minimumShouldMatch(1)
                                )
                                .explain(true)
                );
        printSearchResponse(searchRequest);
    }

    @DisplayName("초성 검색을 위한 인덱스 생성")
    @Test
    void create_index_for_chosung_search() throws Exception {
        removeIndexIfExists(AC_TEST_3, dockerClient);
        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();
        {
            builder.field(NUMBER_OF_SHARDS, 5);
            builder.field(NUMBER_OF_REPLICAS, 1);
            builder.field(MAX_NGRAM_DIFF, 50);

            builder.startObject("analysis");
            {
                builder.startObject("analyzer");
                {
                    builder.startObject("chosung_index_analyzer");
                    {
                        builder.field("type", CUSTOM);
                        builder.field("tokenizer", KEYWORD);
                        builder.startArray("filter");
                        {
                            builder.value("javacafe_chosung_filter")
                                    .value(LOWERCASE)
                                    .value(TRIM)
                                    .value("edge_ngram_filter_front");
                        }
                        builder.endArray();
                    }
                    builder.endObject();

                    builder.startObject("chosung_search_analyzer");
                    {
                        builder.field("type", CUSTOM);
                        //TODO keyword 토크나이저 : 전체 입력 문자열을 하나의 키워드처럼 처리한다. 즉, 토큰화 처리를 하지 않는다.
                        //  얼핏 보면 쓸데없어 보이지만, 통계 중 집계 사용시 필수적으로 들어가야 하는 속성으로, 주로 통계가 필요한 항목(국가, 성별 등) 에 쓰인다.
                        builder.field("tokenizer", KEYWORD);
                        builder.startArray("filter");
                        {
                            builder.value("javacafe_chosung_filter")
                                    .value(LOWERCASE)
                                    .value(TRIM);
                        }
                        builder.endArray();
                    }
                    builder.endObject();
                }
                builder.endObject();

                builder.startObject("tokenizer");
                {
                    builder.startObject("edge_ngram_tokenizer");
                    {
                        builder.field("type", "edge_ngram");
                        builder.field("min_gram", 1);
                        builder.field("max_gram", 50);
                        builder.startArray("token_chars");
                        {
                            builder.value(LETTER)
                                    .value(DIGIT)
                                    .value(PUNCTUATION)
                                    .value(SYMBOL);
                        }
                        builder.endArray();
                    }
                    builder.endObject();
                }
                builder.endObject();


                builder.startObject("filter");
                {
                    builder.startObject("edge_ngram_filter_front");
                    {
                        builder.field("type", "edge_ngram");
                        builder.field("min_gram", 1);
                        builder.field("max_gram", 50);
                        builder.field("side", "front");
                    }
                    builder.endObject();


                    builder.startObject("javacafe_chosung_filter");
                    {
                        builder.field("type", "javacafe_chosung");
                    }
                    builder.endObject();
                }
                builder.endObject();
            }
            builder.endObject();
        }
        builder.endObject();

        CreateIndexRequest createIndexRequest = new CreateIndexRequest(AC_TEST_3)
                .settings(builder);

        CreateIndexResponse createIndexResponse = dockerClient.indices().create(createIndexRequest, RequestOptions.DEFAULT);
        assertCreatedIndex(createIndexResponse, AC_TEST_3);
    }

    @DisplayName("매핑 설정")
    @Test
    void mapping_for_chosung_search() throws Exception {
        create_index_for_chosung_search();

        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();
        {
            builder.startObject("properties");
            {
                //TODO 일밙벅인 매칭 검색 용도로 사용하는 필드다. 필드를 정의할 때 keyword 데이터 타입으로 설정한다.
                builder.startObject("item");
                {
                    builder.field("type", KEYWORD);
                    builder.field("boost", 30);
                }
                builder.endObject();

                builder.startObject("itemChosung");
                {
                    //TODO 초성으로 분석된 정보를 자동완성으로 매칭하기 위한 필드다. 필드를 정의할 때 index_analyzer 와 search_analyzer 를 각가 다르게 등록해야 한다.
                    // 색인할 때는 chosung_index_analyzer 를 분석기로 사용하고, 검색할 때는 chosung_search_analyzer 를 분석기로 사용한다.
                    builder.field("type", "text");
                    builder.field("analyzer", "chosung_index_analyzer");
                    builder.field("search_analyzer", "chosung_search_analyzer");
                    builder.field("boost", 10);
                }
                builder.endObject();
            }
            builder.endObject();
        }
        builder.endObject();

        PutMappingRequest putMappingRequest = new PutMappingRequest(AC_TEST_3)
                .source(builder);

        AcknowledgedResponse putMappingResponse = dockerClient.indices().putMapping(putMappingRequest, RequestOptions.DEFAULT);
        assertTrue(putMappingResponse.isAcknowledged());
    }


    @DisplayName("자동완성 데이터 색인")
    @Test
    void index_for_chosung_auto_completion() throws Exception {
        mapping_for_chosung_search();
        BulkRequest bulkRequest = new BulkRequest()
                .add(
                        new IndexRequest(AC_TEST_3)
                                .id("1")
                                .source(Map.of("item", "신혼", "itemChosung", "신혼"))
                )
                .add(
                        new IndexRequest(AC_TEST_3)
                                .id("2")
                                .source(Map.of("item", "신혼가전", "itemChosung", "신혼가전"))
                )
                .add(
                        new IndexRequest(AC_TEST_3)
                                .id("3")
                                .source(Map.of("item", "신혼가전특별전", "itemChosung", "신혼가전특별전"))
                );


        BulkResponse bulkResponse = dockerClient.bulk(bulkRequest, RequestOptions.DEFAULT);
        assertBulkIndexResponse(bulkResponse);
        Thread.sleep(1500);
    }


    @DisplayName("초성 검색 수행")
    @Test
    void search_with_chosung() throws Exception {
        index_for_chosung_auto_completion();

        SearchRequest searchRequest
                = new SearchRequest(AC_TEST_3)
                .source(
                        new SearchSourceBuilder()
                                .query(
                                        boolQuery()
                                                .should(
                                                        termQuery("itemChosung", "ㅅㅎㄱㅈ")
                                                )
                                                .minimumShouldMatch(1)
                                )
                );


        printSearchResponse(searchRequest);
    }

    @DisplayName("자모로 분석하기 위한 인덱스 생성")
    @Test
    void create_index_for_jamo_analyzer() throws Exception {
        removeIndexIfExists(AC_TEST_4, dockerClient);

        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();
        {
            builder.field(NUMBER_OF_SHARDS, 5);
            builder.field(NUMBER_OF_REPLICAS, 1);
            builder.field(MAX_NGRAM_DIFF, 50);
            builder.startObject("analysis");
            {
                builder.startObject("analyzer");
                {
                    //TODO 색인용 분석기
                    builder.startObject("jamo_index_analyzer");
                    {
                        builder.field("type", CUSTOM);
                        builder.field("tokenizer", KEYWORD);
                        builder.startArray("filter");
                        {
                            builder.value("javacafe_jamo_filter")
                                    .value(LOWERCASE)
                                    .value(TRIM)
                                    .value("edge_ngram_filter_front");
                        }
                        builder.endArray();
                    }
                    builder.endObject();

                    //TODO 검색용 분석기
                    builder.startObject("jamo_search_analyzer");
                    {
                        builder.field("type", CUSTOM);
                        builder.field("tokenizer", KEYWORD);
                        builder.startArray("filter");
                        {
                            builder.value("javacafe_jamo_filter")
                                    .value(LOWERCASE)
                                    .value(TRIM);
                        }
                        builder.endArray();
                    }
                    builder.endObject();


                }
                builder.endObject();


                builder.startObject("tokenizer");
                {
                    builder.startObject("edge_ngram_tokenizer");
                    {
                        builder.field("type", "edge_ngram");
                        builder.field("min_gram", 1);
                        builder.field("max_gram", 50);
                        builder.startArray("filter");
                        {
                            builder.value(LETTER)
                                    .value(DIGIT)
                                    .value(PUNCTUATION)
                                    .value(SYMBOL);
                        }
                        builder.endArray();
                    }
                    builder.endObject();
                }
                builder.endObject();


                builder.startObject("filter");
                {
                    builder.startObject("edge_ngram_filter_front");
                    {
                        builder.field("type", "edge_ngram");
                        builder.field("min_gram", 1);
                        builder.field("max_gram", 50);
                        builder.field("side", "front");
                    }
                    builder.endObject();

                    builder.startObject("javacafe_jamo_filter");
                    {
                        builder.field("type", "javacafe_jamo");
                    }
                    builder.endObject();
                }
                builder.endObject();

            }
            builder.endObject();
        }
        builder.endObject();

        CreateIndexRequest createIndexRequest = new CreateIndexRequest(AC_TEST_4)
                .settings(builder);

        CreateIndexResponse createIndexResponse = dockerClient.indices().create(createIndexRequest, RequestOptions.DEFAULT);
        assertCreatedIndex(createIndexResponse, AC_TEST_4);
    }


    @DisplayName("매핑 설정")
    @Test
    void setting_mapping_for_jamo_analyzer() throws Exception {
        create_index_for_jamo_analyzer();
        XContentBuilder builder = XContentFactory.jsonBuilder();

        builder.startObject();
        {
            builder.startObject("properties");
            {
                //TODO 일반적인 매칭 검색 용도로만 사용하는 필드다. 필드를 정의할 때 keyword 타입으로 설정한다.
                builder.startObject("item");
                {
                    builder.field("type", KEYWORD);
                    builder.field("boost", 30);
                    builder.startArray("copy_to");
                    {
                        builder.value("itemJamo");
                    }
                    builder.endArray();
                }
                builder.endObject();

                //TODO 자모 분석된 정보를 자동완성으로 매칭하기 위한 필드다. 필드를 정의할 때 index_analyzer 와 search_analyzer 를 각각 다르게 등록한다.
                // 색인 시에는 jamo_index_analyzer 를 분석기로 사용하고, 검색할 때는 jamo_search_analyzer 를 분석기로 사용한다.
                builder.startObject("itemJamo");
                {
                    builder.field("type", "text");
                    builder.field("analyzer", "jamo_index_analyzer");
                    builder.field("search_analyzer", "jamo_search_analyzer");
                    builder.field("boost", 10);
                }
                builder.endObject();
            }
            builder.endObject();
        }
        builder.endObject();


        PutMappingRequest putMappingRequest = new PutMappingRequest(AC_TEST_4)
                .source(builder);

        AcknowledgedResponse putMappingResponse = dockerClient.indices().putMapping(putMappingRequest, RequestOptions.DEFAULT);
        assertTrue(putMappingResponse.isAcknowledged());
    }

    @DisplayName("자동완성 데이터 색인")
    @Test
    void index_data_for_jamo_analyzer() throws Exception {
        setting_mapping_for_jamo_analyzer();
        BulkRequest bulkRequest = new BulkRequest()
                .add(
                        new IndexRequest(AC_TEST_4)
                                .id("1")
                                .source(Map.of("item", "신혼"))
                ).add(
                        new IndexRequest(AC_TEST_4)
                                .id("2")
                                .source(Map.of("item", "신혼가전"))
                ).add(
                        new IndexRequest(AC_TEST_4)
                                .id("3")
                                .source(Map.of("item", "신혼가전특별전"))
                );
        BulkResponse bulkResponse = dockerClient.bulk(bulkRequest, RequestOptions.DEFAULT);
        assertBulkIndexResponse(bulkResponse);
        Thread.sleep(1500);
    }

    @DisplayName("자모 분석 실행")
    @Test
    void search_with_jamo_analyzer() throws Exception {
        index_data_for_jamo_analyzer();
        SearchRequest searchRequest = new SearchRequest(AC_TEST_4)
                .source(
                        new SearchSourceBuilder()
                                .query(
                                        boolQuery()
                                                .should(
                                                        termQuery("itemJamo", "ㅅㅣㄴㅎㅗ")
                                                )
                                                .minimumShouldMatch(1)
                                )
                );

        printSearchResponse(searchRequest);
    }



}
