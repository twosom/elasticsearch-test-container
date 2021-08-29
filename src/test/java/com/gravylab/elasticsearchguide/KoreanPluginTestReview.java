package com.gravylab.elasticsearchguide;

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
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.suggest.SuggestBuilder;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.elasticsearch.index.query.QueryBuilders.*;
import static org.elasticsearch.search.suggest.SuggestBuilders.completionSuggestion;
import static org.elasticsearch.search.suggest.SuggestBuilders.termSuggestion;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 반드시 ElasticSearch 노드에 javacafe plugin 이 설치되어 있어야 한다.<br/>
 * 테스트 환경 : 7.12.0
 */
public class KoreanPluginTestReview extends CommonDockerClass {
    public static final String AC_TEST = "ac_test";
    public static final String ITEM_SRC = "itemSrc";
    public static final String ITEM_COMPLETION = "itemCompletion";
    public static final String S_1 = "s1";
    public static final String AC_TEST_2 = "ac_test2";
    public static final String NGRAM_ANALYZER = "ngram_analyzer";
    public static final String NGRAM_TOKENIZER = "ngram_tokenizer";
    public static final String EDGE_NGRAM_ANALYZER = "edge_ngram_analyzer";
    public static final String EDGE_NGRAM_ANALYZER_BACK = EDGE_NGRAM_ANALYZER + "_back";
    public static final String EDGE_NGRAM_TOKENIZER = "edge_ngram_tokenizer";
    public static final String EDGE_NGRAM_FILTER_FRONT = "edge_ngram_filter_front";
    public static final String EDGE_NGRAM_FILTER_BACK = "edge_ngram_filter_back";
    public static final String MIN_GRAM = "min_gram";
    public static final String MAX_GRAM = "max_gram";
    public static final String TOKEN_CHARS = "token_chars";
    public static final String SYMBOL = "symbol";
    public static final String PUNCTUATION = "punctuation";
    public static final String DIGIT = "digit";
    public static final String LETTER = "letter";
    public static final String NGRAM = "ngram";
    public static final String EDGE_NGRAM = "edge_ngram";
    public static final String FRONT = "front";
    public static final String SIDE = "side";
    public static final String BACK = "back";
    public static final String ITEM = "item";
    public static final String BOOST = "boost";
    public static final String ITEM_NGRAM = "itemNgram";
    public static final String ITEM_NGRAM_EDGE = "itemNgramEdge";
    public static final String ITEM_NGRAM_EDGE_BACK = "itemNgramEdgeBack";
    public static final String CHOSUNG_INDEX_ANALYZER = "chosung_index_analyzer";
    public static final String JAVACAFE_CHOSUNG_FILTER = "javacafe_chosung_filter";
    public static final String CHOSUNG_SEARCH_ANALYZER = "chosung_search_analyzer";
    public static final String JAVACAFE_CHOSUNG = "javacafe_chosung";
    public static final String AC_TEST_3 = "ac_test3";
    public static final String ITEM_CHOSUNG = "itemChosung";
    public static final String JAMO_INDEX_ANALYZER = "jamo_index_analyzer";
    public static final String JAVACAFE_JAMO_FILTER = "javacafe_jamo_filter";
    public static final String JAMO_SEARCH_ANALYZER = "jamo_search_analyzer";
    public static final String JAVACAFE_JAMO = "javacafe_jamo";
    public static final String AC_TEST_4 = "ac_test4";
    public static final String ITEM_JAMO = "itemJamo";


    //TODO Completion Suggest API 를 이용해 자동완성을 제공하는 예제 만들기

    @DisplayName("인덱스 생성")
    @Test
    void create_index_for_korean_auto_completion() throws Exception {
        removeIndexIfExists(AC_TEST, client);

        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();
        {
            builder.field(NUMBER_OF_SHARDS, 5);
            builder.field(NUMBER_OF_REPLICAS, 1);
        }
        builder.endObject();
        CreateIndexRequest createIndexRequest = new CreateIndexRequest(AC_TEST)
                .settings(builder);

        CreateIndexResponse createIndexResponse = client.indices().create(createIndexRequest, RequestOptions.DEFAULT);
        assertCreatedIndex(createIndexResponse, AC_TEST);
    }

    @DisplayName("매핑 설정")
    @Test
    void mapping_for_korean_auto_completion() throws Exception {
        create_index_for_korean_auto_completion();
        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();
        {
            builder.startObject(PROPERTIES);
            {
                //TODO 일반적인 매칭 검색 용도로 사용하는 필드다. Keyword 데이터 타입으로 설정한다.
                builder.startObject(ITEM_SRC);
                {
                    builder.field(TYPE, KEYWORD);
                    builder.startArray(COPY_TO);
                    {
                        builder.value(ITEM_COMPLETION);
                    }
                    builder.endArray();
                }
                builder.endObject();

                //TODO 자동완성 용도로 사용하는 필드다. 필드를 정의할 때 Completion 데이터 타입으로 설정한다.
                builder.startObject(ITEM_COMPLETION);
                {
                    builder.field(TYPE, COMPLETION);
                }
                builder.endObject();
            }
            builder.endObject();
        }
        builder.endObject();

        PutMappingRequest putMappingRequest = new PutMappingRequest(AC_TEST)
                .source(builder);

        AcknowledgedResponse putMappingResponse = client.indices().putMapping(putMappingRequest, RequestOptions.DEFAULT);
        assertTrue(putMappingResponse.isAcknowledged());
    }


    @DisplayName("자동완성용 데이터 색인하기")
    @Test
    void index_data_for_korean_auto_completion() throws Exception {
        mapping_for_korean_auto_completion();
        BulkRequest bulkRequest = new BulkRequest()
                .add(
                        new IndexRequest(AC_TEST)
                                .id("1")
                                .source(Map.of(ITEM_SRC, "신혼"))
                )
                .add(
                        new IndexRequest(AC_TEST)
                                .id("2")
                                .source(Map.of(ITEM_SRC, "신혼가전"))
                )
                .add(
                        new IndexRequest(AC_TEST)
                                .id("3")
                                .source(Map.of(ITEM_SRC, "신혼가전특별전"))
                );

        BulkResponse bulkResponse = client.bulk(bulkRequest, RequestOptions.DEFAULT);
        assertBulkIndexResponse(bulkResponse);
        Thread.sleep(1500);
    }


    @DisplayName("itemCompletion 필드를 이용한 자동완성 검색")
    @Test
    void search_korean_auto_completion_with_item_completion() throws Exception {
        index_data_for_korean_auto_completion();
        SearchRequest searchRequest = new SearchRequest(AC_TEST)
                .source(
                        new SearchSourceBuilder()
                                .suggest(
                                        new SuggestBuilder()
                                                .addSuggestion(S_1,
                                                        completionSuggestion(ITEM_COMPLETION)
                                                                .prefix("신혼")
                                                                .size(10)
                                                )

                                )
                );


        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
        printSuggestText(searchResponse, S_1);

        //TODO 위에서 엘라스틱서치에서 제공하는 Completion Suggest API 기능을 이용해 자동완성을 손쉽게 구현했다.
        // 하지만 여기서 구현한 자동완성 기능에는 몇가지 미흡한 점이 있다.


        //TODO
        // 1) 부분일치 불가
        // 첫 번째 문제는 키워드의 일부분으로 자동완성의 결과가 제공되지 않는다는 점이다.
        // 앞서 살펴본 예제에서는 키워드로 "신혼" 이라는 단어를 사용했고, 결과로 "신혼", "신혼가전", "산혼가전특별전" 이라는 매우 흡족한 결과를 얻었다.
        // 하지만 "가전" 이라는 단어를 검색어로 이용한다면 사용자는 아마도 "신혼가전", "신혼가전특별전" 이라는 2건의 문서가 결과로 제공되길 원할 것이다.
        searchRequest = new SearchRequest(AC_TEST)
                .source(
                        new SearchSourceBuilder()
                                .suggest(
                                        new SuggestBuilder()
                                                .addSuggestion(S_1,
                                                        completionSuggestion(ITEM_COMPLETION)
                                                                .prefix("가전")
                                                                .size(10)
                                                )
                                )
                );

        searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
        printSuggestText(searchResponse, S_1);

        //TODO 실망스럽게도 결과는 0건이다.
        // 자동완성 결과로 아무것도 리턴되지 않았다. 그 이유는 Completion Suggest API 가 내부적으로 Prefix 방식의 매칭만 지원하고 있어
        // 키워드의 시작 부분이 반드시 일치해야 결과로 제공하기 때문이다.
        // Completion Suggest API 를 이용해 자동완성을 구현할 경우 반드시 키워드의 시작어를 알고 사용해야 한다.
        // 하지만 사용자들은 어떤 키워드가 있는지 알 길이 없다. 자동완성 서비스를 제공하려면 키워드 기준으로 시작, 중간, 끝 등의 전방위 검색이 모두 가능해야 한다.
    }


    //TODO Ngram 이 글자 단위로 토큰을 생성하기 때문에 Ngram 분석기, Edge Ngram 분석기, Edge Ngram Back 분석기라는 총 세 가지 분석기를 모두 사용하면 어떠한 부분일치도 구현할 수 있다.
    @DisplayName("한글 자동완성을 위한 인덱스 생성")
    @Test
    void create_index_for_korean_ngram_auto_completion() throws Exception {
        removeIndexIfExists(AC_TEST_2, client);
        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();
        {
            builder.field(NUMBER_OF_SHARDS, 5);
            builder.field(NUMBER_OF_REPLICAS, 1);
            builder.field(MAX_NGRAM_DIFF, 50);

            builder.startObject(ANALYSIS);
            {
                builder.startObject(ANALYZER);
                {

                    //TODO ngram_analyzer 정의5
                    builder.startObject(NGRAM_ANALYZER);
                    {
                        builder.field(TYPE, CUSTOM);
                        builder.field(TOKENIZER, NGRAM_TOKENIZER);
                        builder.startArray(FILTER);
                        {
                            builder.value(LOWERCASE)
                                    .value(TRIM);
                        }
                        builder.endArray();
                    }
                    builder.endObject();

                    //TODO edge_ngram_analyzer 정의
                    builder.startObject(EDGE_NGRAM_ANALYZER);
                    {
                        builder.field(TYPE, CUSTOM);
                        builder.field(TOKENIZER, EDGE_NGRAM_TOKENIZER);
                        builder.startArray(FILTER);
                        {
                            builder.value(LOWERCASE)
                                    .value(TRIM)
                                    .value(EDGE_NGRAM_FILTER_FRONT);
                        }
                        builder.endArray();
                    }
                    builder.endObject();

                    //TODO edge_ngram_analyzer_back 정의
                    builder.startObject(EDGE_NGRAM_ANALYZER_BACK);
                    {
                        builder.field(TYPE, CUSTOM);
                        builder.field(TOKENIZER, EDGE_NGRAM_TOKENIZER);
                        builder.startArray(FILTER);
                        {
                            builder.value(LOWERCASE)
                                    .value(TRIM)
                                    .value(EDGE_NGRAM_FILTER_BACK);
                        }
                        builder.endArray();
                    }
                    builder.endObject();

                }
                builder.endObject();


                builder.startObject(TOKENIZER);
                {
                    //TODO ngram_tokenizer 정의
                    builder.startObject(NGRAM_TOKENIZER);
                    {
                        builder.field(TYPE, NGRAM);
                        builder.field(MIN_GRAM, 1);
                        builder.field(MAX_GRAM, 50);
                        builder.startArray(TOKEN_CHARS);
                        {
                            builder.value(LETTER)
                                    .value(DIGIT)
                                    .value(PUNCTUATION)
                                    .value(SYMBOL);
                        }
                        builder.endArray();
                    }
                    builder.endObject();

                    //TODO edge_ngram_tokenizer 정의
                    builder.startObject(EDGE_NGRAM_TOKENIZER);
                    {
                        builder.field(TYPE, EDGE_NGRAM);
                        builder.field(MIN_GRAM, 1);
                        builder.field(MAX_GRAM, 50);
                        builder.startArray(TOKEN_CHARS);
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


                builder.startObject(FILTER);
                {
                    //TODO edge_ngram_filter_front 정의
                    builder.startObject(EDGE_NGRAM_FILTER_FRONT);
                    {
                        builder.field(TYPE, EDGE_NGRAM);
                        builder.field(MIN_GRAM, 1);
                        builder.field(MAX_GRAM, 50);
                        builder.field(SIDE, FRONT);
                    }
                    builder.endObject();

                    //TODO edge_ngram_filter_back 정의
                    builder.startObject(EDGE_NGRAM_FILTER_BACK);
                    {
                        builder.field(TYPE, EDGE_NGRAM);
                        builder.field(MIN_GRAM, 1);
                        builder.field(MAX_GRAM, 50);
                        builder.field(SIDE, BACK);
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


        CreateIndexResponse createIndexResponse = client.indices().create(createIndexRequest, RequestOptions.DEFAULT);
        assertCreatedIndex(createIndexResponse, AC_TEST_2);
    }

    @DisplayName("매핑 설정")
    @Test
    void mapping_for_ngram_analyzer() throws Exception {
        create_index_for_korean_ngram_auto_completion();
        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();
        {
            builder.startObject(PROPERTIES);
            {
                //TODO 일반적인 매칭 검색 용도로 사용하는 필드. keyword 타입으로 설정
                builder.startObject(ITEM);
                {
                    builder.field(TYPE, KEYWORD);
                    builder.field(BOOST, 30);
                    builder.startArray(COPY_TO);
                    {
                        builder.value(ITEM_NGRAM)
                                .value(ITEM_NGRAM_EDGE)
                                .value(ITEM_NGRAM_EDGE_BACK);
                    }
                    builder.endArray();
                }
                builder.endObject();

                //TODO Ngram 으로 분석된 정보를 자동완성으로 매칭하기 위한 필드. 필드를 정의할 때 ngram_analyzer 를 분석기로 사용한다.
                builder.startObject(ITEM_NGRAM);
                {
                    builder.field(TYPE, TEXT);
                    builder.field(ANALYZER, NGRAM_ANALYZER);
                    builder.field(SEARCH_ANALYZER, NGRAM_ANALYZER);
                    builder.field(BOOST, 3);
                }
                builder.endObject();

                //TODO Edge Ngram 으로 분석된 정보를 자동완성으로 매칭하기 위한 필드.
                // 필드를 정의할 때 index_analyzer 와 search_analyzer 를 각각 다르게 등록한다.
                // 색인할 때는 edge_ngram_analyzer 를 분석기로 사용하고, 검색할 때는 ngram_analyzer 를 분석기로 사용한다.
                builder.startObject(ITEM_NGRAM_EDGE);
                {
                    builder.field(TYPE, TEXT);
                    builder.field(ANALYZER, EDGE_NGRAM_ANALYZER);
                    builder.field(SEARCH_ANALYZER, NGRAM_ANALYZER);
                    builder.field(BOOST, 2);
                }
                builder.endObject();

                //TODO Edge Ngram Back 으로 분석된 정보를 자동완성으로 매칭하기 위한 필드다.
                // 필드를 정의할 때 index_analyzer 와 search_analyzer 를 각각 다르게 등록한다.
                // 색인할 때는 edge_ngram_analyzer_back 분석기로 사용하고 검색할 때는 ngram_analyzer 분석기로 사용한다.
                builder.startObject(ITEM_NGRAM_EDGE_BACK);
                {
                    builder.field(TYPE, TEXT);
                    builder.field(ANALYZER, EDGE_NGRAM_ANALYZER_BACK);
                    builder.field(SEARCH_ANALYZER, NGRAM_ANALYZER);
                    builder.field(BOOST, 1);
                }
                builder.endObject();

            }
            builder.endObject();
        }
        builder.endObject();

        PutMappingRequest putMappingRequest = new PutMappingRequest(AC_TEST_2)
                .source(builder);

        AcknowledgedResponse putMappingResponse = client.indices().putMapping(putMappingRequest, RequestOptions.DEFAULT);
        assertTrue(putMappingResponse.isAcknowledged());
    }


    @DisplayName("자동완성 데이터 색인")
    @Test
    void index_data_for_ngram_analyzer() throws Exception {
        mapping_for_ngram_analyzer();
        BulkRequest bulkRequest = new BulkRequest()
                .add(
                        new IndexRequest(AC_TEST_2)
                                .id("1")
                                .source(Map.of(ITEM, "신혼"))
                ).add(
                        new IndexRequest(AC_TEST_2)
                                .id("2")
                                .source(Map.of(ITEM, "신혼가전"))
                ).add(
                        new IndexRequest(AC_TEST_2)
                                .id("3")
                                .source(Map.of(ITEM, "신혼가전특별전"))
                );


        BulkResponse bulkResponse = client.bulk(bulkRequest, RequestOptions.DEFAULT);
        assertBulkIndexResponse(bulkResponse);
        Thread.sleep(1500);
    }

    @DisplayName("전방일치 자동완성 요청")
    @Test
    void request_for_prefix_auto_completion_with_ngram_analyzer() throws Exception {
        index_data_for_ngram_analyzer();
        //TODO Completion Suggest API 기능을 이용해 자동완성을 구현한 경우 부분일치가 되지 않는다는 문제가 있었다.
        // 일반적인 인덱스와 동일하게 search API 를 이용해 검색을 수행하면 된다.
        // 검색할 때 모든 필드를 대상으로 term 쿼리를 수행해서 하나라도 매칭되는 경우 자동완성 결과로 제공하면 된다.

        String keyword = "가전";
        SearchRequest searchRequest = new SearchRequest(AC_TEST_2)
                .source(
                        new SearchSourceBuilder()
                                .query(
                                        boolQuery()
                                                .should(
                                                        prefixQuery(ITEM, keyword)
                                                )
                                                .should(
                                                        termQuery(ITEM, keyword)
                                                )
                                                .should(
                                                        termQuery(ITEM_NGRAM, keyword)
                                                )
                                                .should(
                                                        termQuery(ITEM_NGRAM_EDGE, keyword)
                                                )
                                                .should(
                                                        termQuery(ITEM_NGRAM_EDGE_BACK, keyword)
                                                )
                                                .minimumShouldMatch(1)
                                )
                );


        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
        printSearchResponse(searchResponse);
        //TODO 원하는 검색 결과는 나왔지만 랭킹의 변화가 필요하다.
        // "신혼" 이라는 단어로 검색했고, 검색 결과를 보면 "신혼" 이라고 돼 있는 문서는 맨 마지막에 위치해 있다.
        // "신혼" 이라는 문서를 가장 먼저 올리고 싶다면 완전일치되는 단어를 먼저 검색되게 만들면 된다.
    }


    @DisplayName("한글 초성 검색을 위한 인덱스 생성")
    @Test
    void create_index_for_chosung_analyzer() throws Exception {
        removeIndexIfExists(AC_TEST_3, client);
        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();
        {
            builder.field(NUMBER_OF_SHARDS, 5);
            builder.field(NUMBER_OF_REPLICAS, 1);
            builder.field(MAX_NGRAM_DIFF, 50);

            builder.startObject(ANALYSIS);
            {
                builder.startObject(ANALYZER);
                {
                    builder.startObject(CHOSUNG_INDEX_ANALYZER);
                    {
                        builder.field(TYPE, CUSTOM);
                        builder.field(TOKENIZER, KEYWORD);
                        builder.startArray(FILTER);
                        {
                            builder.value(JAVACAFE_CHOSUNG_FILTER)
                                    .value(LOWERCASE)
                                    .value(TRIM)
                                    //TODO 인덱스 시에는 edge_ngram_filter_front 필터를 적용한다.
                                    .value(EDGE_NGRAM_FILTER_FRONT)
                                    .value(EDGE_NGRAM_FILTER_BACK)
                                    ;
                        }
                        builder.endArray();
                    }
                    builder.endObject();

                    builder.startObject(CHOSUNG_SEARCH_ANALYZER);
                    {
                        builder.field(TYPE, CUSTOM);
                        builder.field(TOKENIZER, KEYWORD);
                        builder.startArray(FILTER);
                        {
                            builder.value(JAVACAFE_CHOSUNG_FILTER)
                                    .value(LOWERCASE)
                                    .value(TRIM);
                        }
                        builder.endArray();
                    }
                    builder.endObject();

                }
                builder.endObject();

                builder.startObject(TOKENIZER);
                {
                    builder.startObject(EDGE_NGRAM_TOKENIZER);
                    {
                        builder.field(TYPE, EDGE_NGRAM);
                        builder.field(MIN_GRAM, 1);
                        builder.field(MAX_GRAM, 50);
                        builder.startArray(TOKEN_CHARS);
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

                builder.startObject(FILTER);
                {
                    builder.startObject(EDGE_NGRAM_FILTER_FRONT);
                    {
                        builder.field(TYPE, EDGE_NGRAM);
                        builder.field(MIN_GRAM, 1);
                        builder.field(MAX_GRAM, 50);
                        builder.field(SIDE, FRONT);
                    }
                    builder.endObject();

                    builder.startObject(EDGE_NGRAM_FILTER_BACK);
                    {
                        builder.field(TYPE, EDGE_NGRAM);
                        builder.field(MIN_GRAM, 1);
                        builder.field(MAX_GRAM, 50);
                        builder.field(SIDE, BACK);
                    }
                    builder.endObject();



                    builder.startObject(JAVACAFE_CHOSUNG_FILTER);
                    {
                        builder.field(TYPE, JAVACAFE_CHOSUNG);
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

        CreateIndexResponse createIndexResponse = client.indices().create(createIndexRequest, RequestOptions.DEFAULT);
        assertCreatedIndex(createIndexResponse, AC_TEST_3);
    }


    @DisplayName("매핑 설정")
    @Test
    void mapping_for_korean_chosung_analyzer() throws Exception {
        create_index_for_chosung_analyzer();
        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();
        {
            builder.startObject(PROPERTIES);
            {
                builder.startObject(ITEM);
                {
                    builder.field(TYPE, KEYWORD);
                    builder.field(BOOST, 30);
                    builder.startArray(COPY_TO);
                    {
                        builder.value(ITEM_CHOSUNG);
                    }
                    builder.endArray();
                }
                builder.endObject();

                builder.startObject(ITEM_CHOSUNG);
                {
                    builder.field(TYPE, TEXT);
                    builder.field(ANALYZER, CHOSUNG_INDEX_ANALYZER);
                    builder.field(SEARCH_ANALYZER, CHOSUNG_SEARCH_ANALYZER);
                    builder.field(BOOST, 10);
                }
                builder.endObject();
            }
            builder.endObject();
        }
        builder.endObject();


        PutMappingRequest putMappingRequest = new PutMappingRequest(AC_TEST_3)
                .source(builder);


        AcknowledgedResponse putMappingResponse = client.indices().putMapping(putMappingRequest, RequestOptions.DEFAULT);
        assertTrue(putMappingResponse.isAcknowledged());
    }


    @DisplayName("자동완성 데이터 색인")
    @Test
    void index_data_for_korean_chosung_analyzer() throws Exception {
        mapping_for_korean_chosung_analyzer();
        BulkRequest bulkRequest = new BulkRequest()
                .add(
                        new IndexRequest(AC_TEST_3)
                                .id("1")
                                .source(Map.of(ITEM, "신혼"))
                )
                .add(
                        new IndexRequest(AC_TEST_3)
                                .id("2")
                                .source(Map.of(ITEM, "신혼가전"))
                )
                .add(
                        new IndexRequest(AC_TEST_3)
                                .id("3")
                                .source(Map.of(ITEM, "신혼가전특별전"))
                );

        BulkResponse bulkResponse = client.bulk(bulkRequest, RequestOptions.DEFAULT);
        assertBulkIndexResponse(bulkResponse);
        Thread.sleep(1500);
    }


    @DisplayName("초성검색 수행")
    @Test
    void search_with_chosung_analyzer() throws Exception {
        index_data_for_korean_chosung_analyzer();
        SearchRequest searchRequest = new SearchRequest(AC_TEST_3)
                .source(
                        new SearchSourceBuilder()
                                .query(
                                        boolQuery()
                                                .should(
                                                        termQuery(ITEM_CHOSUNG, "ㅎㄱ")
                                                )
                                                .minimumShouldMatch(1)
                                )
                );

        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
        printSearchResponse(searchResponse);
    }


    @DisplayName("자모 분석을 위한 인덱스 생성")
    @Test
    void create_index_for_jamo_analyzer() throws Exception {

        removeIndexIfExists(AC_TEST_4, client);
        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();
        {
            builder.field(NUMBER_OF_SHARDS, 5);
            builder.field(NUMBER_OF_REPLICAS, 1);
            builder.field(MAX_NGRAM_DIFF, 50);

            builder.startObject(ANALYSIS);
            {
                builder.startObject(ANALYZER);
                {
                    builder.startObject(JAMO_INDEX_ANALYZER);
                    {
                        builder.field(TYPE, CUSTOM);
                        builder.field(TOKENIZER, KEYWORD);
                        builder.startArray(FILTER);
                        {
                            builder.value(JAVACAFE_JAMO_FILTER)
                                    .value(LOWERCASE)
                                    .value(TRIM)
                                    .value(EDGE_NGRAM_FILTER_FRONT)
                                    .value(EDGE_NGRAM_FILTER_BACK)
                            ;
                        }
                        builder.endArray();
                    }
                    builder.endObject();


                    builder.startObject(JAMO_SEARCH_ANALYZER);
                    {
                        builder.field(TYPE, CUSTOM);
                        builder.field(TOKENIZER, KEYWORD);
                        builder.startArray(FILTER);
                        {
                            builder.value(JAVACAFE_JAMO_FILTER)
                                    .value(LOWERCASE)
                                    .value(TRIM);
                        }
                        builder.endArray();
                    }
                    builder.endObject();

                }
                builder.endObject();


                builder.startObject(TOKENIZER);
                {
                    builder.startObject(EDGE_NGRAM_TOKENIZER);
                    {
                        builder.field(TYPE, EDGE_NGRAM);
                        builder.field(MIN_GRAM, 1);
                        builder.field(MAX_GRAM, 50);
                        builder.startArray(TOKEN_CHARS);
                        {
                            builder.value(LETTER)
                                    .value(DIGIT)
                                    .value(PUNCTUATION)
                                    .value(SYMBOL);
                        }
                        builder.endArray();
                    }
                    builder.endObject();


                    builder.startObject(NGRAM_TOKENIZER);
                    {
                        builder.field(TYPE, NGRAM);
                        builder.field(MIN_GRAM, 1);
                        builder.field(MAX_GRAM, 50);
                        builder.startArray(TOKEN_CHARS);
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

                builder.startObject(FILTER);
                {
                    builder.startObject(EDGE_NGRAM_FILTER_FRONT);
                    {
                        builder.field(TYPE, EDGE_NGRAM);
                        builder.field(MIN_GRAM, 1);
                        builder.field(MAX_GRAM, 50);
                        builder.field(SIDE, FRONT);
                    }
                    builder.endObject();

                    builder.startObject(EDGE_NGRAM_FILTER_BACK);
                    {
                        builder.field(TYPE, EDGE_NGRAM);
                        builder.field(MIN_GRAM, 1);
                        builder.field(MAX_GRAM, 50);
                        builder.field(SIDE, BACK);
                    }
                    builder.endObject();

                    builder.startObject(JAVACAFE_JAMO_FILTER);
                    {
                        builder.field(TYPE, JAVACAFE_JAMO);
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

        CreateIndexResponse createIndexResponse = client.indices().create(createIndexRequest, RequestOptions.DEFAULT);
        assertCreatedIndex(createIndexResponse, AC_TEST_4);
    }

    @DisplayName("매핑 설정")
    @Test
    void mapping_for_jamo_analyzer() throws Exception {
        create_index_for_jamo_analyzer();
        XContentBuilder builder = XContentFactory.jsonBuilder();

        builder.startObject();
        {
            builder.startObject(PROPERTIES);
            {
                //TODO 일반적인 매칭 검색 용도로 사용하는 필드. 필드를 정의할 때 keyword 타입으로 설정한다.
                builder.startObject(ITEM);
                {
                    builder.field(TYPE, KEYWORD);
                    builder.field(BOOST, 30);
                    builder.startArray(COPY_TO);
                    {
                        builder.value(ITEM_JAMO);
                    }
                    builder.endArray();
                }
                builder.endObject();

                //TODO 자모 분석된 정보를 자동완성으로 매칭하기 위한 필드.
                // 필드를 정의할 때 index_analyzer 와 search_analyzer 를 각각 다르게 등록한다.
                // 색인할 때는 jamo_index_analyzer 를 분석기로 사용하고, 검색할 때는 jamo_search_analyzer 를 분석기로 사용한다.
                builder.startObject(ITEM_JAMO);
                {
                    builder.field(TYPE, TEXT);
                    builder.field(ANALYZER, JAMO_INDEX_ANALYZER);
                    builder.field(SEARCH_ANALYZER, JAMO_SEARCH_ANALYZER);
                    builder.field(BOOST, 10);
                }
                builder.endObject();
            }
            builder.endObject();
        }
        builder.endObject();


        PutMappingRequest putMappingRequest = new PutMappingRequest(AC_TEST_4)
                .source(builder);

        AcknowledgedResponse putMappingResponse = client.indices().putMapping(putMappingRequest, RequestOptions.DEFAULT);
        assertTrue(putMappingResponse.isAcknowledged());
    }

    @DisplayName("자동완성 데이터 색인")
    @Test
    void index_data_for_jamo_analyzer() throws Exception {
        mapping_for_jamo_analyzer();

        BulkRequest bulkRequest = new BulkRequest()
                .add(
                        new IndexRequest(AC_TEST_4)
                                .id("1")
                                .source(Map.of(ITEM, "신혼"))
                ).add(
                        new IndexRequest(AC_TEST_4)
                                .id("2")
                                .source(Map.of(ITEM, "신혼가전"))
                ).add(
                        new IndexRequest(AC_TEST_4)
                                .id("3")
                                .source(Map.of(ITEM, "신혼가전특별전"))
                );

        BulkResponse bulkResponse = client.bulk(bulkRequest, RequestOptions.DEFAULT);
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
                                                        termQuery(ITEM_JAMO, "ㅈㅓㄴㅌ")
                                                )
                                                .minimumShouldMatch(1)
                                )
                );

        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
        printSearchResponse(searchResponse);
    }



}
