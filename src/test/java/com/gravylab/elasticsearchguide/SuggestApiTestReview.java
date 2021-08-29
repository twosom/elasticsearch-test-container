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
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.suggest.Suggest;
import org.elasticsearch.search.suggest.SuggestBuilder;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.text.Normalizer;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import static org.elasticsearch.index.query.QueryBuilders.matchQuery;
import static org.elasticsearch.search.suggest.SuggestBuilders.completionSuggestion;
import static org.elasticsearch.search.suggest.SuggestBuilders.termSuggestion;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class SuggestApiTestReview extends CommonDockerClass {


    // TODO Term Suggest API : 잘못된 철자에 대해 해당 인덱스의 특정 필드에서 가장 유사한 단어 추천
    //          Completion Suggest API : 사용자가 입력을 완료하기 전에 자동완성을 사용해 검색어를 예측해서 보여줌.


    //TODO Term Suggest API : 편집거리(edit distance) 를 사용해 비슷한 단어를 제안한다.
    // 편집거리 척도란 어떤 문자열이 다른 문자열과 얼마나 비슷한가를 편집거리를 사용해 알아볼 수 있으며, 두 문자열 사이의 편집거리는
    // 하나의 문자열을 다른 문자열로 바꾸는데 필요한 편집 횟수를 말한다.

    //TODO 편집거리를 측정하는 방식은 대부분 각 단어를 삽입, 삭제, 치환하는 연산을 포함한다. 삽입이란 원본 문자열에 문자 한 개를 추가해서 원본과 검색어가 더 비슷하게 만드는 작업.
    // 삭제는 문자를 한 개 삭제, 치환은 원본 문자열 한 개를 대상 문자 한 개와 치환하는 것을 의미.
    // 이러한 연산을 조합해서 척도를 측정한다.

    @DisplayName("테스트를 위해 데이터 생성")
    @Test
    void index_data_for_test() throws Exception {
        removeIndexIfExists(MOVIE_TERM_SUGGEST, client);
        BulkRequest bulkRequest = new BulkRequest()
                .add(
                        new IndexRequest(MOVIE_TERM_SUGGEST)
                                .id("1")
                                .source(Map.of(MOVIE_NM, "lover"))
                ).add(
                        new IndexRequest(MOVIE_TERM_SUGGEST)
                                .id("2")
                                .source(Map.of(MOVIE_NM, "Fall love"))
                ).add(
                        new IndexRequest(MOVIE_TERM_SUGGEST)
                                .id("3")
                                .source(Map.of(MOVIE_NM, "lovely"))
                ).add(
                        new IndexRequest(MOVIE_TERM_SUGGEST)
                                .id("4")
                                .source(Map.of(MOVIE_NM, "lovestory"))
                );


        BulkResponse bulkResponse = client.bulk(bulkRequest, RequestOptions.DEFAULT);
        assertBulkIndexResponse(bulkResponse);
        Thread.sleep(1500);
    }


    @DisplayName("suggest 기능을 이용하여 비슷한 단어 추천")
    @Test
    void test_suggest_api() throws Exception {
        index_data_for_test();
        SearchRequest searchRequest = new SearchRequest(MOVIE_TERM_SUGGEST)
                .source(
                        new SearchSourceBuilder()
                                .suggest(
                                        new SuggestBuilder()
                                                .addSuggestion(SPELL_SUGGESTION,
                                                        termSuggestion(MOVIE_NM)
                                                                .text("lave")
                                                )
                                )
                );

        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
        printSuggestText(searchResponse, SPELL_SUGGESTION);
    }

    @DisplayName("자동완성 기능을 사용하기 위한 인덱스 생성")
    @Test
    void create_index_for_completion_suggest_api() throws Exception {
        removeIndexIfExists(MOVIE_TERM_COMPLETION, client);

        XContentBuilder builder = XContentFactory.jsonBuilder();

        builder.startObject();
        {
            builder.startObject(PROPERTIES);
            {
                builder.startObject(MOVIE_NM_EN_COMPLE);
                {
                    builder.field(TYPE, COMPLETION);
                }
                builder.endObject();
            }
            builder.endObject();
        }
        builder.endObject();

        CreateIndexRequest createIndexRequest = new CreateIndexRequest(MOVIE_TERM_COMPLETION)
                .mapping(builder);

        CreateIndexResponse createIndexResponse = client.indices().create(createIndexRequest, RequestOptions.DEFAULT);
        assertCreatedIndex(createIndexResponse, MOVIE_TERM_COMPLETION);
    }


    @DisplayName("위에서 생성된 인덱스에 문서를 추가")
    @Test
    void index_data_for_completion_suggest_api() throws Exception {
        create_index_for_completion_suggest_api();
        BulkRequest bulkRequest = new BulkRequest()
                .add(
                        new IndexRequest(MOVIE_TERM_COMPLETION)
                                .id("1")
                                .source(Map.of(MOVIE_NM_EN_COMPLE, "After Love"))
                ).add(
                        new IndexRequest(MOVIE_TERM_COMPLETION)
                                .id("2")
                                .source(Map.of(MOVIE_NM_EN_COMPLE, "Lover"))
                ).add(
                        new IndexRequest(MOVIE_TERM_COMPLETION)
                                .id("3")
                                .source(Map.of(MOVIE_NM_EN_COMPLE, "Love for a mother"))
                ).add(
                        new IndexRequest(MOVIE_TERM_COMPLETION)
                                .id("4")
                                .source(Map.of(MOVIE_NM_EN_COMPLE, "Fall love"))
                ).add(
                        new IndexRequest(MOVIE_TERM_COMPLETION)
                                .id("5")
                                .source(Map.of(MOVIE_NM_EN_COMPLE, "My lovely wife"))
                );

        BulkResponse bulkResponse = client.bulk(bulkRequest, RequestOptions.DEFAULT);
        assertBulkIndexResponse(bulkResponse);
        Thread.sleep(1500);
    }

    @DisplayName("위에서 생성된 데이터로 자동완성 기능 테스트")
    @Test
    void test_for_completion_suggest_api() throws Exception {
        index_data_for_completion_suggest_api();
        SearchRequest searchRequest = new SearchRequest(MOVIE_TERM_COMPLETION)
                .source(
                        new SearchSourceBuilder()
                                .suggest(
                                        new SuggestBuilder()
                                                .addSuggestion(MOVIE_COMPLETION,
                                                        completionSuggestion(MOVIE_NM_EN_COMPLE)
                                                                .prefix("l")
                                                )
                                )
                                .size(5)
                );

        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
        printSuggestText(searchResponse, MOVIE_COMPLETION);
    }

    //TODO 부분일치를 하고 싶다면 부분일치가 되어 나와씅면 하는 부분을 분리해서 배열 형태로 만들어야 한다.

    @DisplayName("기존 데이터들을 모두 지우고 새롭게 문서를 만든다")
    @Test
    void reindex_data_for_completion_suggest_api() throws Exception {
        create_index_for_completion_suggest_api();
        BulkRequest bulkRequest = new BulkRequest()
                .add(
                        new IndexRequest(MOVIE_TERM_COMPLETION)
                                .id("1")
                                .source(
                                        Map.of(MOVIE_NM_EN_COMPLE, Map.of("input", List.of("After", "Love")))
                                )
                ).add(
                        new IndexRequest(MOVIE_TERM_COMPLETION)
                                .id("2")
                                .source(
                                        Map.of(MOVIE_NM_EN_COMPLE, Map.of("input", List.of("Lover")))
                                )
                ).add(
                        new IndexRequest(MOVIE_TERM_COMPLETION)
                                .id("3")
                                .source(
                                        Map.of(MOVIE_NM_EN_COMPLE, Map.of("input", List.of("Love", "for", "a", "mother")))
                                )
                ).add(
                        new IndexRequest(MOVIE_TERM_COMPLETION)
                                .id("4")
                                .source(
                                        Map.of(MOVIE_NM_EN_COMPLE, Map.of("input", List.of("Fall", "Love")))
                                )
                ).add(
                        new IndexRequest(MOVIE_TERM_COMPLETION)
                                .id("5")
                                .source(
                                        Map.of(MOVIE_NM_EN_COMPLE, Map.of("input", List.of("My", "lovely", "wife")))
                                )
                );

        BulkResponse bulkResponse = client.bulk(bulkRequest, RequestOptions.DEFAULT);
        assertBulkIndexResponse(bulkResponse);
        Thread.sleep(1500);
    }

    @DisplayName("위에서 재생성한 데이터로 다시 한번 검색")
    @Test
    void retest_for_completion_suggest_api() throws Exception {
        reindex_data_for_completion_suggest_api();
        SearchRequest searchRequest = new SearchRequest(MOVIE_TERM_COMPLETION)
                .source(
                        new SearchSourceBuilder()
                                .suggest(
                                        new SuggestBuilder()
                                                .addSuggestion(MOVIE_COMPLETION,
                                                        completionSuggestion(MOVIE_NM_EN_COMPLE)
                                                                .prefix("l")
                                                )

                                )
                                .size(5)
                );

        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
        printSuggestText(searchResponse, MOVIE_COMPLETION);
    }

    @DisplayName("한글 맞춤법 검사를 위한 인덱스 생성")
    @Test
    void create_index_for_korean_term_suggest_api() throws Exception {
        removeIndexIfExists(COMPANY_SPELLCHECKER, client);
        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();
        {
            builder.startObject(ANALYSIS);
            {
                builder.startObject(ANALYZER);
                {
                    builder.startObject(KOREAN_SPELL_ANALYZER);
                    {

                        builder.field(TYPE, CUSTOM);
                        builder.field(TOKENIZER, STANDARD);
                        builder.startArray(FILTER);
                        {
                            builder.value(TRIM)
                                    .value(LOWERCASE)
                                    .value(JAVACAFE_SPELL);
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

        CreateIndexResponse createIndexResponse = client.indices().create(createIndexRequest, RequestOptions.DEFAULT);
        assertCreatedIndex(createIndexResponse, COMPANY_SPELLCHECKER);
    }

    @DisplayName("매핑 설정")
    @Test
    void mapping_for_korean_term_suggest_api() throws Exception {
        create_index_for_korean_term_suggest_api();
        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();
        {
            builder.startObject(PROPERTIES);
            {
                builder.startObject(NAME);
                {
                    builder.field(TYPE, KEYWORD);
                    builder.startArray(COPY_TO);
                    {
                        builder.value(SUGGEST);
                    }
                    builder.endArray();
                }
                builder.endObject();

                builder.startObject(SUGGEST);
                {
                    builder.field(TYPE, COMPLETION);
                    builder.field(ANALYZER, KOREAN_SPELL_ANALYZER);
                }
                builder.endObject();
            }
            builder.endObject();
        }
        builder.endObject();

        PutMappingRequest putMappingRequest = new PutMappingRequest(COMPANY_SPELLCHECKER)
                .source(builder);

        AcknowledgedResponse putMappingResponse = client.indices().putMapping(putMappingRequest, RequestOptions.DEFAULT);
        assertTrue(putMappingResponse.isAcknowledged());
    }


    @DisplayName("한글 오타 교정 테스트용 데이터 색인")
    @Test
    void index_data_for_korean_term_suggest_api() throws Exception {
        mapping_for_korean_term_suggest_api();
        IndexRequest indexRequest = new IndexRequest(COMPANY_SPELLCHECKER)
                .id("1")
                .source(Map.of(NAME, "삼성전자"));

        client.index(indexRequest, RequestOptions.DEFAULT);
        Thread.sleep(1500);
    }

    @DisplayName("오타 교정 API 요청")
    @Test
    void request_for_korean_term_suggest_api() throws Exception {
        index_data_for_korean_term_suggest_api();
        SearchRequest searchRequest = new SearchRequest(COMPANY_SPELLCHECKER)
                .source(
                        new SearchSourceBuilder()
                                .suggest(
                                        new SuggestBuilder()
                                                .addSuggestion(MY_SUGGESTION,
                                                        termSuggestion(SUGGEST)
                                                                .text("샴성전자")
                                                )
                                )
                );

        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
        String keyword = searchResponse
                .getSuggest()
                .getSuggestion(MY_SUGGESTION)
                .getEntries()
                .get(0)
                .getOptions()
                .get(0)
                .getText()
                .toString();
        String normalizedText = Normalizer.normalize(keyword, Normalizer.Form.NFC);
        System.err.println("normalizedText = " + normalizedText);
        //TODO 사용자가 입력한 단어와 비슷한 단어를 찾기 위해 javacafe_spell 필터는 내부적으로 색인된 모든 데이터를 자소 단위로 분해해서 생성한다.
        // 이때는 모든 데이터가 자소 단위로 분해됐기 때문에 편집거리 계산이 가능해진다.


        //TODO 보통 사용자가 검색할 때 검색 결과가 한 건도 나오지 않거나 전체 건수의 1 ~ 2 % 미만으로 나오는 경우에는
        // 이러한 오타 교정 API 를 호출해서 교정된 검색어를 추가로 제시하거나, 교정된 검색어로 검색한 결과를 출력한다.
    }


    @DisplayName("한영 오타 교정을 위한 인덱스 생성")
    @Test
    void create_index_for_kor_eng_term_suggest() throws Exception {
        //TODO 여기서는 2개의 인덱스를 생성해야 한다.
        // 먼저 company 라는 이름으로 인덱스를 생성하고 일반적인 분석기를 추가한다.

        removeIndexIfExists(COMPANY, client);
        removeIndexIfExists(SEARCH_KEYWORD, client);

        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();
        {
            builder.startObject(ANALYSIS);
            {
                builder.startObject(ANALYZER);
                {
                    builder.startObject(MY_ANALYZER);
                    {
                        builder.field(TYPE, CUSTOM);
                        builder.field(TOKENIZER, STANDARD);
                        builder.startArray(FILTER);
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

        CreateIndexResponse createIndexResponse = client.indices().create(createIndexRequest, RequestOptions.DEFAULT);
        assertCreatedIndex(createIndexResponse, COMPANY);


        //TODO 두 번째로 search_keyword 라는 인덱스를 생성한다.
        // 한영 오차교정과 영한 오차교정을 위해 각각 분석기를 설정한다.
        // 한영오차교정용 분석기인 kor2eng_analyzer 분석기의 filter 항목에는 자바카페 플러그인으로 제공하는 필터 중 javacafe_kor2eng 필터를 추가한다.
        // 영한 오차교정용 분석기인 eng2kor_analyzer 분석기의 filter 항목에는 javacafe_eng2kor 필터를 추가한다.
        // 이 인덱스는 한영/영한 오타가 있는지 검사하는 용도로 사용될 것이다.
        builder = XContentFactory.jsonBuilder();

        builder.startObject();
        {
            builder.startObject(ANALYSIS);
            {
                builder.startObject(ANALYZER);
                {
                    builder.startObject(KOR_2_ENG_ANALYZER);
                    {
                        builder.field(TYPE, CUSTOM);
                        builder.field(TOKENIZER, STANDARD);
                        builder.startArray(FILTER);
                        {
                            builder.value(TRIM)
                                    .value(LOWERCASE)
                                    .value(JAVACAFE_KOR_2_ENG);
                        }
                        builder.endArray();
                    }
                    builder.endObject();


                    builder.startObject(ENG_2_KOR_ANALYZER);
                    {
                        builder.field(TYPE, CUSTOM);
                        builder.field(TOKENIZER, STANDARD);
                        builder.startArray(FILTER);
                        {
                            builder.value(TRIM)
                                    .value(LOWERCASE)
                                    .value(JAVACAFE_ENG_2_KOR);
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

        createIndexResponse = client.indices().create(createIndexRequest, RequestOptions.DEFAULT);
        assertCreatedIndex(createIndexResponse, SEARCH_KEYWORD);
    }


    @DisplayName("매핑 설정")
    @Test
    void mapping_for_kor_eng_term_suggest() throws Exception {
        create_index_for_kor_eng_term_suggest();

        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();
        {
            builder.startObject(PROPERTIES);
            {
                builder.startObject(NAME);
                {
                    builder.field(TYPE, KEYWORD);
                }
                builder.endObject();
            }
            builder.endObject();
        }
        builder.endObject();


        PutMappingRequest putMappingRequest = new PutMappingRequest(COMPANY)
                .source(builder);

        AcknowledgedResponse putMappingResponse = client.indices().putMapping(putMappingRequest, RequestOptions.DEFAULT);
        assertTrue(putMappingResponse.isAcknowledged());


        builder = XContentFactory.jsonBuilder();
        builder.startObject();
        {
            builder.startObject(PROPERTIES);
            {
                builder.startObject(NAME);
                {
                    builder.field(TYPE, KEYWORD);
                    builder.startArray(COPY_TO);
                    {
                        builder.value(KOR_2_ENG_SUGGEST)
                                .value(ENG_2_KOR_SUGGEST);
                    }
                    builder.endArray();
                }
                builder.endObject();


                builder.startObject(KOR_2_ENG_SUGGEST);
                {
                    builder.field(TYPE, TEXT);
                    //TODO 문서를 색인할 때는 원래 방식대로
                    builder.field(ANALYZER, STANDARD);
                    //TODO 검색할 때만 오타 교정 필터가 적용되도록 별도록 search_analyzer 를 설정하는것이 중요
                    builder.field(SEARCH_ANALYZER, KOR_2_ENG_ANALYZER);
                }
                builder.endObject();

                builder.startObject(ENG_2_KOR_SUGGEST);
                {
                    builder.field(TYPE, TEXT);
                    builder.field(ANALYZER, STANDARD);
                    builder.field(SEARCH_ANALYZER, ENG_2_KOR_ANALYZER);
                }
                builder.endObject();
            }
            builder.endObject();
        }
        builder.endObject();

        putMappingRequest = new PutMappingRequest(SEARCH_KEYWORD)
                .source(builder);

        putMappingResponse = client.indices().putMapping(putMappingRequest, RequestOptions.DEFAULT);
        assertTrue(putMappingResponse.isAcknowledged());
    }


    @DisplayName("오타 교정 데이터 색인")
    @Test
    void index_data_for_kor_eng_term_suggest() throws Exception {
        mapping_for_kor_eng_term_suggest();
        BulkRequest bulkRequest = new BulkRequest()
                .add(
                        new IndexRequest(COMPANY)
                                .id("1")
                                .source(Map.of(NAME, "삼성전자"))
                )
                .add(
                        new IndexRequest(COMPANY)
                                .id("2")
                                .source(Map.of(NAME, "iphone"))
                )
                .add(
                        new IndexRequest(SEARCH_KEYWORD)
                                .id("1")
                                .source(Map.of(NAME, "삼성전자"))
                )
                .add(
                        new IndexRequest(SEARCH_KEYWORD)
                                .id("2")
                                .source(Map.of(NAME, "iphone"))
                );

        BulkResponse bulkResponse = client.bulk(bulkRequest, RequestOptions.DEFAULT);
        assertBulkIndexResponse(bulkResponse);
        Thread.sleep(1500);
    }

    //TODO 오타 교정 API 요청
    // 오타 교정은 보통 다음과 같이 동작한다. 먼저 오타 교정 API 를 실행하고 결과에 따라 실제 인덱스를 검색하기 위한 검색어를 결정한다.
    // 1. search_keyword 인덱스에 사용자 검색어를 가지고 검색 질의를 한다.
    // 2. 검색 결과가 없다면 검색어 그대로 company 인덱스에 검색 질의를 한다.
    // 3. 검색 결과가 있다면 변경된 검색어로 company 인덱스에 검색 질의를 한다.

    @DisplayName("한영 오타 테스트")
    @Test
    void test_for_kor_eng_term_suggest() throws Exception {
        index_data_for_kor_eng_term_suggest();

        SearchRequest searchRequest = new SearchRequest(SEARCH_KEYWORD)
                .source(
                        new SearchSourceBuilder()
                                .query(
                                        matchQuery(ENG_2_KOR_SUGGEST, "tkatjdwjswk")
                                                .queryName("query")
                                )
                );

        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
        printSearchResponse(searchResponse);
    }


    @DisplayName("영한 오타 테스트")
    @Test
    void test_for_eng_kor_term_suggest() throws Exception {
        index_data_for_kor_eng_term_suggest();
        SearchRequest searchRequest = new SearchRequest(SEARCH_KEYWORD)
                .source(
                        new SearchSourceBuilder()
                                .query(
                                        matchQuery(KOR_2_ENG_SUGGEST, "ㅑㅔㅗㅐㅜㄷ")
                                                .queryName("query")
                                )
                );

        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
        printSearchResponse(searchResponse);
    }

    @DisplayName("종합 오타 테스트")
    @ParameterizedTest(name = "검색어 : {0}")
    @ValueSource(strings = {"tkatjdwjswk", "ㅑㅔㅙㅜㄷ"})
    void all_test_for_term_suggest(String keyword) throws Exception {
        index_data_for_kor_eng_term_suggest();
        SearchRequest searchRequest = null;
        if (isEnglish(keyword)) {
            searchRequest = new SearchRequest(SEARCH_KEYWORD)
                    .source(
                            new SearchSourceBuilder()
                                    .query(
                                            matchQuery(ENG_2_KOR_SUGGEST, keyword)
                                    )
                                    .suggest(
                                            new SuggestBuilder()
                                                    .addSuggestion(ENGLISH_TO_KOREAN_SUGGEST,
                                                            termSuggestion(ENG_2_KOR_SUGGEST)
                                                                    .text(keyword)
                                                    )
                                    )
                    );
            SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
            printSearchResponse(searchResponse);
            printSuggestText(searchResponse, ENGLISH_TO_KOREAN_SUGGEST);
            TimeValue took = searchResponse.getTook();
            System.err.println("took = " + took);
        } else if (isKorean(keyword)) {
            searchRequest = new SearchRequest(SEARCH_KEYWORD)
                    .source(
                            new SearchSourceBuilder()
                                    .query(
                                            matchQuery(KOR_2_ENG_SUGGEST, keyword)
                                    )
                                    .suggest(
                                            new SuggestBuilder()
                                                    .addSuggestion(KOREAN_TO_ENGLISH_SUGGEST,
                                                            termSuggestion(KOR_2_ENG_SUGGEST)
                                                                    .text(keyword)
                                                    )
                                    )
                    );
            SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
            printSearchResponse(searchResponse);
            printSuggestText(searchResponse, KOREAN_TO_ENGLISH_SUGGEST);
            TimeValue took = searchResponse.getTook();
            System.err.println("took = " + took);
        }
    }

    private boolean isEnglish(String keyword) {
        return Pattern.matches("^[a-zA-Z]*$", keyword);
    }

    private boolean isKorean(String keyword) {
        return Pattern.matches("^[ㄱ-ㅣ가-힣]*$", keyword);
    }

}
