package com.gravylab.elasticsearchguide;

import org.apache.http.HttpHost;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.suggest.Suggest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.io.IOException;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Testcontainers
public class CommonDockerClass {


    public static final String NUMBER_OF_SHARDS = "number_of_shards";
    public static final String NUMBER_OF_REPLICAS = "number_of_replicas";
    public static final String MOVIE_TERM_SUGGEST = "movie_term_suggest";
    public static final String SPELL_SUGGESTION = "spell-suggestion";
    public static final String MOVIE_TERM_COMPLETION = "movie_term_completion";
    public static final String MOVIE_NM_EN_COMPLE = "movieNmEnComple";
    public static final String COMPLETION = "completion";
    public static final String MOVIE_COMPLETION = "movie_completion";
    public static final String CUSTOM = "custom";
    public static final String STANDARD = "standard";
    public static final String JAVACAFE_SPELL = "javacafe_spell";
    public static final String LOWERCASE = "lowercase";
    public static final String TRIM = "trim";
    public static final String COMPANY_SPELLCHECKER = "company_spellchecker";
    public static final String KOREAN_SPELL_ANALYZER = "korean_spell_analyzer";
    public static final String PROPERTIES = "properties";
    public static final String NAME = "name";
    public static final String TYPE = "type";
    public static final String KEYWORD = "keyword";
    public static final String COPY_TO = "copy_to";
    public static final String SUGGEST = "suggest";
    public static final String ANALYZER = "analyzer";
    public static final String TOKENIZER = "tokenizer";
    public static final String FILTER = "filter";
    public static final String ANALYSIS = "analysis";
    public static final String MY_SUGGESTION = "my-suggestion";
    public static final String MOVIE_NM = "movieNm";
    public static final String MY_ANALYZER = "my_analyzer";
    public static final String COMPANY = "company";
    public static final String KOR_2_ENG_ANALYZER = "kor2eng_analyzer";
    public static final String ENG_2_KOR_ANALYZER = "eng2kor_analyzer";
    public static final String JAVACAFE_KOR_2_ENG = "javacafe_kor2eng";
    public static final String JAVACAFE_ENG_2_KOR = "javacafe_eng2kor";
    public static final String SEARCH_KEYWORD = "search_keyword";
    public static final String KOR_2_ENG_SUGGEST = "kor2eng_suggest";
    public static final String ENG_2_KOR_SUGGEST = "eng2kor_suggest";
    public static final String TEXT = "text";
    public static final String SEARCH_ANALYZER = "search_analyzer";
    public static final String KOREAN_TO_ENGLISH_SUGGEST = "koreanToEnglishSuggest";
    public static final String ENGLISH_TO_KOREAN_SUGGEST = "englishToKoreanSuggest";
    public static final String MAX_NGRAM_DIFF = "max_ngram_diff";

    RestHighLevelClient client;

    @BeforeEach
    void beforeEach() {
        this.client = getDockerRestClient();
    }

    private RestHighLevelClient getDockerRestClient() {
        RestClientBuilder builder = RestClient.builder(
                HttpHost.create("localhost:9200")
        );
        return new RestHighLevelClient(
                builder
                        .setHttpClientConfigCallback(
                                httpClientBuilder ->
                                        httpClientBuilder
                                                .setConnectionReuseStrategy((response, context) -> true)
                                                .setKeepAliveStrategy((response, context) -> 9999999999L)
                        )
        );
    }

    @AfterEach
    void afterEach() throws IOException {
        client.close();
    }

    protected void removeIndexIfExists(String index, RestHighLevelClient client) throws IOException {
        if (isExistsIndex(index, client)) {
            DeleteIndexRequest deleteIndexRequest = new DeleteIndexRequest(index);
            AcknowledgedResponse deleteResponse = client.indices().delete(deleteIndexRequest, RequestOptions.DEFAULT);
            Assertions.assertTrue(deleteResponse.isAcknowledged());
            System.out.println("[" + index + "] 인덱스 삭제가 완료되었습니다.");
        }
    }

    protected void assertBulkIndexResponse(BulkResponse bulkResponse) {
        boolean result = Arrays.stream(bulkResponse.getItems())
                .map(BulkItemResponse::getOpType)
                .allMatch(e -> e.equals(DocWriteRequest.OpType.INDEX));
        Assertions.assertTrue(result);
    }

    protected void printSearchResponse(SearchResponse searchResponse) {
        Arrays.stream(searchResponse
                        .getInternalResponse()
                        .hits()
                        .getHits())
                .map(SearchHit::getSourceAsMap)
                .forEach(System.err::println);
    }

    protected boolean isExistsIndex(String index, RestHighLevelClient client) throws IOException {
        GetIndexRequest getIndexRequest = new GetIndexRequest(index);
        return client.indices().exists(getIndexRequest, RequestOptions.DEFAULT);
    }

    protected void printSuggestText(SearchResponse searchResponse, String suggestName) {
        searchResponse
                .getSuggest()
                .getSuggestion(suggestName)
                .getEntries()
                .get(0)
                .getOptions()
                .stream()
                .map(Suggest.Suggestion.Entry.Option::getText)
                .forEach(System.err::println);
    }


    /**
     * {@link RestHighLevelClient} 로 ElasticSearch 의 인덱스 생성 시 정상적으로 생성됬는지 검증하는 메소드이다.
     *
     * @param createIndexResponse 인덱스 생성 후 넘겨받은 {@link CreateIndexResponse} 객체를 넘겨준다.
     * @param indexName           생성한 인덱스 이름을 넘겨준다.
     */
    protected void assertCreatedIndex(CreateIndexResponse createIndexResponse, String indexName) {
        assertTrue(createIndexResponse.isAcknowledged());
        assertTrue(createIndexResponse.isShardsAcknowledged());
        assertEquals(createIndexResponse.index(), indexName);
    }
}
