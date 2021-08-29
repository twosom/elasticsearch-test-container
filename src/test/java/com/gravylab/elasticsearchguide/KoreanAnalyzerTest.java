package com.gravylab.elasticsearchguide;

import org.elasticsearch.action.admin.cluster.storedscripts.DeleteStoredScriptRequest;
import org.elasticsearch.action.admin.cluster.storedscripts.GetStoredScriptRequest;
import org.elasticsearch.action.admin.cluster.storedscripts.GetStoredScriptResponse;
import org.elasticsearch.action.admin.cluster.storedscripts.PutStoredScriptRequest;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.indices.AnalyzeRequest;
import org.elasticsearch.client.indices.AnalyzeResponse;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.index.query.QueryBuilders.matchQuery;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 테스트 환경 : ElasticSearch 7.14.0
 * 사전에 Docker 를 실행 후 각각의 인스턴스에 analysis-nori 플러그인을 설치해준다.
 */
public class KoreanAnalyzerTest extends CommonTestClass {


    public static final String INDEX_NAME = "nori_analyzer";
    public static final String TOKENIZER_NAME = "nori_user_dict_tokenizer";
    public static final String DECOMPOUND_MODE = "decompound_mode";
    public static final String ANALYZER_NAME = "nori_token_analyzer";
    public static final String NORI_READINGFORM = "nori_readingform";
    public static final String MOVIE_HIGHLIGHTING = "movie_highlighting";
    public static final String MOVIE_SCRIPT = "movie_script";

    @DisplayName("인덱스 생성 시 사용자 정의사전 추가")
    @Test
    void index_with_user_dictionary() throws Exception {
        if (isExistsIndex(INDEX_NAME, dockerClient)) {
            return;
        }

        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();
        {
            builder.startObject("analysis");
            {
                //TODO Tokenizer 정의
                builder.startObject("tokenizer");
                {
                    builder.startObject(TOKENIZER_NAME);
                    {
                        builder.field("type", "nori_tokenizer");
                        builder.field(DECOMPOUND_MODE, "mixed");
                        builder.field("user_dictionary", "userdic_ko.txt");
                    }
                    builder.endObject();
                }
                builder.endObject();


                //TODO Analyzer 정의
                builder.startObject("analyzer");
                {
                    builder.startObject(ANALYZER_NAME);
                    {
                        builder.field("type", "custom");
                        builder.field("tokenizer", TOKENIZER_NAME);
                    }
                    builder.endObject();
                }
                builder.endObject();

            }
            builder.endObject();
        }
        builder.endObject();


        CreateIndexRequest createIndexRequest = new CreateIndexRequest(INDEX_NAME)
                .settings(builder);
        CreateIndexResponse createIndexResponse = dockerClient.indices().create(createIndexRequest, RequestOptions.DEFAULT);
        assertTrue(createIndexResponse.isAcknowledged());
    }

    @DisplayName("생성된 nori_analyzer 인덱스에 설정된 nori_tokenizer 테스트")
    @Test
    void nori_analyzer_test() throws Exception {
        index_with_user_dictionary();
        AnalyzeRequest analyzeRequest = AnalyzeRequest.withIndexAnalyzer(INDEX_NAME, ANALYZER_NAME, "잠실역");
        AnalyzeResponse analyzeResponse = dockerClient.indices().analyze(analyzeRequest, RequestOptions.DEFAULT);
        analyzeResponse.getTokens()
                .stream()
                .map(AnalyzeResponse.AnalyzeToken::getTerm)
                .forEach(System.err::println);

    }

    @DisplayName("nori_readingform 토큰 필터")
    @Test
    void analyze_with_nori_readingform() throws Exception {
        removeIndexIfExists(NORI_READINGFORM, dockerClient);

        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();
        {
            builder.startObject("analysis");
            {
                builder.startObject("analyzer");
                {
                    builder.startObject("nori_readingform_analyzer");
                    {
                        builder.field("tokenizer", "nori_tokenizer");
                        builder.startArray("filter");
                        {
                            builder.value("nori_readingform");
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

        CreateIndexRequest createIndexRequest = new CreateIndexRequest(NORI_READINGFORM)
                .settings(builder);

        CreateIndexResponse createIndexResponse = dockerClient.indices().create(createIndexRequest, RequestOptions.DEFAULT);

        //TODO 위에서 생성한 분석기를 통해 테스트

        AnalyzeRequest analyzeRequest = AnalyzeRequest.withIndexAnalyzer(NORI_READINGFORM, "nori_readingform_analyzer", "中國");
        AnalyzeResponse analyzeResponse = dockerClient.indices().analyze(analyzeRequest, RequestOptions.DEFAULT);
        analyzeResponse.getTokens()
                .stream()
                .map(AnalyzeResponse.AnalyzeToken::getTerm)
                .forEach(System.err::println);
    }


    @DisplayName("하이라이팅 기능")
    @Test
    void search_with_highlighting() throws Exception {
        removeIndexIfExists(MOVIE_HIGHLIGHTING, dockerClient);
        //TODO 하이라이트 기능을 테스트하기 위한 데이터 생성
        Map<String, Object> source = new HashMap<String, Object>();
        source.put("title", "Harry Potter and the Deathly Hallows");
        IndexRequest indexRequest = new IndexRequest()
                .index(MOVIE_HIGHLIGHTING)
                .source(source);
        dockerClient.index(indexRequest, RequestOptions.DEFAULT);

        Thread.sleep(2000);

        //TODO 데이터를 검색할 때 Highlight 옵션을 이용해 하이라이트를 수행할 필드를 지정하면 검색 결과로 하이라이트된 데이터의 일부가 함께 리턴된다.
        HighlightBuilder.Field title = new HighlightBuilder.Field("title");
        HighlightBuilder field = new HighlightBuilder()
                .field(title);

        SearchRequest searchRequest = new SearchRequest(MOVIE_HIGHLIGHTING)
                .source(
                        new SearchSourceBuilder()
                                .query(
                                        matchQuery("title", "harry")
                                                .queryName("query")
                                )
                                .highlighter(field)
                );

        printHighlight(searchRequest);
    }

    private void printHighlight(SearchRequest searchRequest) throws IOException {
        SearchResponse searchResponse = dockerClient.search(searchRequest, RequestOptions.DEFAULT);
        Arrays.stream(searchResponse
                        .getInternalResponse()
                        .hits()
                        .getHits())
                .map(SearchHit::getHighlightFields)
                .forEach(System.err::println);
    }

    @DisplayName("하이라이트 태그 변경")
    @Test
    void change_highlight_tag() throws Exception {
        removeIndexIfExists(MOVIE_HIGHLIGHTING, dockerClient);
        //TODO 하이라이트 기능을 테스트하기 위한 데이터 생성
        Map<String, Object> source = new HashMap<String, Object>();
        source.put("title", "Harry Potter and the Deathly Hallows");
        IndexRequest indexRequest = new IndexRequest()
                .index(MOVIE_HIGHLIGHTING)
                .source(source);
        dockerClient.index(indexRequest, RequestOptions.DEFAULT);

        Thread.sleep(2000);

        SearchRequest searchRequest = new SearchRequest(MOVIE_HIGHLIGHTING)
                .source(
                        new SearchSourceBuilder()
                                .query(
                                        matchQuery("title", "harry")
                                                .queryName("query")
                                )
                );


        searchRequest
                .source()
                .highlighter(
                        new HighlightBuilder()
                                .field(new HighlightBuilder.Field("title"))
                )
                .highlighter()
                .preTags("<strong>")
                .postTags("</strong>")
        ;


        printHighlight(searchRequest);
    }


    @DisplayName("스크립팅을 인덱스 생성")
    @Test
    void index_for_scripting() throws Exception {
        removeIndexIfExists(MOVIE_SCRIPT, dockerClient);

        HashMap<String, Object> source = new HashMap<>();
        source.put("movieList",
                Map.of(
                        "Death_With", 5.5,
                        "About_Time", 7,
                        "Suits", 3.5
                )
        );
        IndexRequest indexRequest = new IndexRequest(MOVIE_SCRIPT)
                .id("1")
                .source(source);

        dockerClient.index(indexRequest, RequestOptions.DEFAULT);
        Thread.sleep(1500);
    }


    @DisplayName("스크립팅을 이용한 필드 추가")
    @Test
    void add_field_using_script() throws Exception {
        index_for_scripting();
        UpdateRequest updateRequest = new UpdateRequest(MOVIE_SCRIPT, "1")
                .script(
                        new Script(ScriptType.INLINE, "painless", "ctx._source.movieList.Black_Panther = 3.7", new HashMap<>())
                );

        dockerClient.update(updateRequest, RequestOptions.DEFAULT);

        GetRequest getRequest = new GetRequest(MOVIE_SCRIPT, "1");

        GetResponse getResponse = dockerClient.get(getRequest, RequestOptions.DEFAULT);
        getResponse.getSourceAsMap()
                .entrySet()
                .forEach(System.err::println);
    }

    @DisplayName("스크립팅을 이용한 필드 제거")
    @Test
    void remove_field_using_script() throws Exception {
        index_for_scripting();
        UpdateRequest updateRequest = new UpdateRequest(MOVIE_SCRIPT, "1")
                .script(
                        new Script(ScriptType.INLINE, "painless", "ctx._source.movieList.remove(\"Suits\")", new HashMap<>())
                );

        dockerClient.update(updateRequest, RequestOptions.DEFAULT);
        GetRequest getRequest = new GetRequest(MOVIE_SCRIPT, "1");
        GetResponse getResponse = dockerClient.get(getRequest, RequestOptions.DEFAULT);

        getResponse.getSourceAsMap()
                .entrySet()
                .forEach(System.err::println);
    }

    @DisplayName("Script API 를 이용해 검색 엔진 템플릿을 위한 템플릿 생성")
    @Test
    void create_template_using_script_api() throws Exception {
        //TODO movie_search_example_template 라는 템플릿이 존재한다면 삭제
        GetStoredScriptResponse getStoredScriptResponse = dockerClient.getScript(
                new GetStoredScriptRequest("movie_search_example_template"), RequestOptions.DEFAULT
        );
        System.out.println("getStoredScriptResponse = " + getStoredScriptResponse);


        DeleteStoredScriptRequest deleteStoredScriptRequest = new DeleteStoredScriptRequest("movie_search_example_template");
        AcknowledgedResponse response = dockerClient.deleteScript(deleteStoredScriptRequest, RequestOptions.DEFAULT);
        System.out.println("response = " + response);


        //TODO script API 를 이용해 템플릿 생성
        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();
        {
            builder.startObject("script");
            {
                builder.field("lang", "mustache");
                builder.startObject("source");
                {
                    builder.startObject("query");
                    {
                        builder.startObject("match");
                        {
                            builder.field("movieNm", "{{movie_name}}");
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

        PutStoredScriptRequest putStoredScriptRequest = new PutStoredScriptRequest()
                .id("movie_search_example_template")
                .content(BytesReference.bytes(builder), XContentType.JSON);

        AcknowledgedResponse acknowledgedResponse = dockerClient.putScript(putStoredScriptRequest, RequestOptions.DEFAULT);
        assertTrue(acknowledgedResponse.isAcknowledged());
    }


}
