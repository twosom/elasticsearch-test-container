package com.gravylab.elasticsearchguide;

import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.PutMappingRequest;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.suggest.SuggestBuilder;
import org.elasticsearch.search.suggest.SuggestBuilders;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.text.Normalizer;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 반드시 ElasticSearch 노드에 javacafe plugin 이 설치되어 있어야 한다.
 */
public class KoreanPluginTest extends CommonTestClass {


    public static final String COMPANY_SPELLCHECKER = "company_spellchecker";
    public static final String KOREAN_SPELL_ANALYZER = "korean_spell_analyzer";

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
                        builder.field("type", "custom");
                        builder.field("tokenizer", "standard");
                        builder.startArray("filter");
                        {
                            builder.value("trim")
                                    .value("lowercase")
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
                    builder.field("type", "keyword");
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
}
