package com.gravylab.elasticsearchguide;

import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.suggest.Suggest;
import org.elasticsearch.search.suggest.SuggestBuilder;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.elasticsearch.search.suggest.SuggestBuilders.completionSuggestion;
import static org.elasticsearch.search.suggest.SuggestBuilders.termSuggestion;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class SuggestApiTest extends CommonTestClass {
    public static final String MOVIE_TERM_SUGGEST = "movie_term_suggest";
    public static final String MOVIE_TERM_COMPLETION = "movie_term_completion";

    //TODO Suggest API
    // Term Suggest API: 추천 단어 제안
    // Completion Suggest API: 자동완성 제안
    // Phrase Suggest API: 추천 문장 제안
    // Context Suggest API: 추천 문맥 제안

    @DisplayName("테스트를 위해 데이터 생성")
    @Test
    void insert_data_for_test() throws Exception {
        removeIndexIfExists(MOVIE_TERM_SUGGEST, testContainerClient);
        BulkRequest bulkRequest = new BulkRequest();
        bulkRequest.add(
                        new IndexRequest()
                                .id("1")
                                .index(MOVIE_TERM_SUGGEST)
                                .source(Map.of("movieNm", "lover"))
                )
                .add(
                        new IndexRequest()
                                .id("2")
                                .index(MOVIE_TERM_SUGGEST)
                                .source(Map.of("movieNm", "Fall love"))
                )
                .add(
                        new IndexRequest()
                                .id("3")
                                .index(MOVIE_TERM_SUGGEST)
                                .source(Map.of("movieNm", "lovely"))
                )
                .add(
                        new IndexRequest()
                                .id("4")
                                .index(MOVIE_TERM_SUGGEST)
                                .source(Map.of("movieNm", "lovestory"))
                );

        BulkResponse bulkResponse = testContainerClient.bulk(bulkRequest, RequestOptions.DEFAULT);
        assertIndexBulkRequest(bulkResponse);
        Thread.sleep(1500);
    }

    private void assertIndexBulkRequest(BulkResponse bulkResponse) {
        boolean result = Arrays.stream(bulkResponse.getItems())
                .map(BulkItemResponse::getOpType)
                .allMatch(e -> e.equals(DocWriteRequest.OpType.INDEX));
        assertTrue(result);
    }

    @DisplayName(MOVIE_TERM_SUGGEST + "에 suggest 기능을 이용해 비슷한 단어 추천하는 로직")
    @Test
    void using_suggest_api() throws Exception {
        insert_data_for_test();

        SearchRequest searchRequest = new SearchRequest(MOVIE_TERM_SUGGEST)
                .source(
                        new SearchSourceBuilder()
                                .suggest(
                                        new SuggestBuilder()
                                                .addSuggestion("spell-suggestion",
                                                        termSuggestion("movieNm")
                                                                .text("lave")
                                                )
                                )
                );


        SearchResponse searchResponse = testContainerClient.search(searchRequest, RequestOptions.DEFAULT);
        searchResponse
                .getInternalResponse()
                .suggest()
                .getSuggestion("spell-suggestion")
                .getEntries()
                .stream()
                .map(Suggest.Suggestion.Entry::getOptions)
                .collect(Collectors.toList())
                .get(0)
                .stream().map(Suggest.Suggestion.Entry.Option::getText)
                .forEach(System.err::println);

    }

    //TODO Completion Suggest API
    // 검색 사이트에서는 일반적으로 사용자의 검색을 효율적으로 돕기 위해 자동완성 기능을 제공한다.
    // 자동완성은 사용자로 하여금 오타를 줄이고 문서 내의 키워들르 미리 보여줌으로써 검색을 조금 더 편하게 사용할 수 있게 도움을 주는 보조 수단이다.
    // 엘라스틱서치에서는 자동완성을 위해 Completion Suggest API 를 제공한다.
    // 자동완성은 글자가 입력될 때마다 검색 결과를 보여줘야 하기 때문에 Term Suggest API 와는 달리 응답속도가 매우 중요하다.
    // 그래서 Completion Suggest API 를 사용하게 되면 엘라스틱서치 내부적으로 FST(Finite State Transducer)를 사용한다.
    // FST 는 검색어가 모두 메모리에 로드되어 서비스되는 구조이며, 즉시 FST 를 로드하게 되면 리소스 측면에서 많은 비용이 한꺼번에 발생하기 떄문에 성능 최적화를 위해 색인 중에 FST 를 작성하게 된다.


    @DisplayName("자동 완성 기능을 위한 인덱스 생성")
    @Test
    void create_index_for_auto_completion() throws Exception {

        removeIndexIfExists(MOVIE_TERM_COMPLETION, testContainerClient);
        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();
        {
            builder.startObject("properties");
            {
                builder.startObject("movieNmEnComple");
                {
                    builder.field("type", "completion");
                }
                builder.endObject();
            }
            builder.endObject();
        }
        builder.endObject();


        CreateIndexRequest createIndexRequest = new CreateIndexRequest(MOVIE_TERM_COMPLETION)
                .mapping(builder);


        CreateIndexResponse createIndexResponse = testContainerClient.indices().create(createIndexRequest, RequestOptions.DEFAULT);
        assertTrue(createIndexResponse.isAcknowledged());

        //TODO 생성된 인덱스에 문서 추가
        BulkRequest bulkRequest = new BulkRequest()
                .add(
                        new IndexRequest()
                                .index(MOVIE_TERM_COMPLETION)
                                .id("1")
                                .source(Map.of("movieNmEnComple", "AfterLove"))
                )
                .add(
                        new IndexRequest()
                                .index(MOVIE_TERM_COMPLETION)
                                .id("2")
                                .source(Map.of("movieNmEnComple", "Lover"))
                )
                .add(
                        new IndexRequest()
                                .index(MOVIE_TERM_COMPLETION)
                                .id("3")
                                .source(Map.of("movieNmEnComple", "Love for a mother"))

                )
                .add(
                        new IndexRequest()
                                .index(MOVIE_TERM_COMPLETION)
                                .id("4")
                                .source(Map.of("movieNmEnComple", "Fall love"))
                )
                .add(
                        new IndexRequest()
                                .index(MOVIE_TERM_COMPLETION)
                                .id("5")
                                .source(Map.of("movieNmEnComple", "My lovely wife"))
                );

        BulkResponse bulkResponse = testContainerClient.bulk(bulkRequest, RequestOptions.DEFAULT);
        assertIndexBulkRequest(bulkResponse);
        Thread.sleep(1500);
    }

    @DisplayName("L 로 시작하는 모든 영화 제목을 검색해보기")
    @Test
    void find_autocompletion_start_with_L() throws Exception {
        create_index_for_auto_completion();

        SearchRequest searchRequest = new SearchRequest(MOVIE_TERM_COMPLETION)
                .source(
                        new SearchSourceBuilder()
                                .suggest(
                                        new SuggestBuilder()
                                                .addSuggestion("movie_completion",
                                                        completionSuggestion("movieNmEnComple")
                                                                .prefix("l")
                                                                .size(5)
                                                )
                                )
                );


        SearchResponse searchResponse = testContainerClient.search(searchRequest, RequestOptions.DEFAULT);
        printAutoCompleList(searchResponse);
    }

    private void printAutoCompleList(SearchResponse searchResponse) {
        searchResponse
                .getInternalResponse()
                .suggest()
                .getSuggestion("movie_completion")
                .getEntries()
                .get(0)
                .getOptions()
                .forEach(System.err::println);
    }


    //TODO 위의 검색 결고로 5개의 Love 라는 단어가 들어가 있는 문서가 모두 나오기를 예상했다.
    // 하지만 두 개의 문서만 검색 결과로 리턴됐는데, 그 이유는 prefix 로 제공되는 전방일치 검색 기능에 의해 L 로 시작하는 단어만 검색되어 나왔기 때문이다.
    // 즉, 영화 제목이 반드시 "L" 로 시작해야만 자동완성 결과로 제공된다.

    @DisplayName("부분일치를 위한 인덱스 생성")
    @Test
    void create_index_for_part_auto_complete() throws Exception {
        removeIndexIfExists(MOVIE_TERM_COMPLETION, testContainerClient);

        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();
        {
            builder.startObject("properties");
            {
                builder.startObject("movieNmEnComple");
                {
                    builder.field("type", "completion");
                }
                builder.endObject();
            }
            builder.endObject();
        }
        builder.endObject();


        CreateIndexRequest createIndexRequest = new CreateIndexRequest(MOVIE_TERM_COMPLETION)
                .mapping(builder);


        CreateIndexResponse createIndexResponse = testContainerClient.indices().create(createIndexRequest, RequestOptions.DEFAULT);
        assertTrue(createIndexResponse.isAcknowledged());




        BulkRequest bulkRequest = new BulkRequest()
                .add(
                        new IndexRequest()
                                .index(MOVIE_TERM_COMPLETION)
                                .id("1")
                                .source(Map.of("movieNmEnComple", Map.of("input", List.of("After", "Love"))))
                )
                .add(
                        new IndexRequest()
                                .index(MOVIE_TERM_COMPLETION)
                                .id("2")
                                .source(Map.of("movieNmEnComple", Map.of("input", List.of("Lover"))))
                )
                .add(
                        new IndexRequest()
                                .index(MOVIE_TERM_COMPLETION)
                                .id("3")
                                .source(Map.of("movieNmEnComple", Map.of("input", List.of("Love", "for", "a", "mother"))))
                )
                .add(
                        new IndexRequest()
                                .index(MOVIE_TERM_COMPLETION)
                                .id("4")
                                .source(Map.of("movieNmEnComple", Map.of("input", List.of("Fall", "love")))
                                ))
                .add(
                        new IndexRequest()
                                .index(MOVIE_TERM_COMPLETION)
                                .id("5")
                                .source(Map.of("movieNmEnComple", Map.of("input", List.of("My", "lovely", "wife"))))
                );

        BulkResponse bulkItemResponses = testContainerClient.bulk(bulkRequest, RequestOptions.DEFAULT);
        assertIndexBulkRequest(bulkItemResponses);
        Thread.sleep(1500);
    }

    @DisplayName("다시 한번 검색해보기")
    @Test
    void find_autocompletion_start_wth_l_again() throws Exception {
        create_index_for_part_auto_complete();

        SearchRequest searchRequest = new SearchRequest(MOVIE_TERM_COMPLETION)
                .source(
                        new SearchSourceBuilder()
                                .suggest(
                                        new SuggestBuilder()
                                                .addSuggestion("movie_completion",
                                                        completionSuggestion("movieNmEnComple")
                                                                .prefix("l")
                                                                .size(5)
                                                )
                                )
                );


        SearchResponse searchResponse = testContainerClient.search(searchRequest, RequestOptions.DEFAULT);
        printAutoCompleList(searchResponse);
    }
}
