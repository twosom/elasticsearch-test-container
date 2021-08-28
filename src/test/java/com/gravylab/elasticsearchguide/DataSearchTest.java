package com.gravylab.elasticsearchguide;

import org.apache.lucene.search.join.ScoreMode;
import org.elasticsearch.action.admin.cluster.repositories.put.PutRepositoryRequest;
import org.elasticsearch.action.admin.cluster.settings.ClusterUpdateSettingsRequest;
import org.elasticsearch.action.admin.cluster.settings.ClusterUpdateSettingsResponse;
import org.elasticsearch.action.admin.cluster.snapshots.get.GetSnapshotsRequest;
import org.elasticsearch.action.admin.cluster.snapshots.get.GetSnapshotsResponse;
import org.elasticsearch.action.admin.cluster.snapshots.restore.RestoreSnapshotRequest;
import org.elasticsearch.action.admin.cluster.snapshots.restore.RestoreSnapshotResponse;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesRequest;
import org.elasticsearch.action.admin.indices.validate.query.ValidateQueryRequest;
import org.elasticsearch.action.admin.indices.validate.query.ValidateQueryResponse;
import org.elasticsearch.action.explain.ExplainRequest;
import org.elasticsearch.action.explain.ExplainResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.*;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.core.CountRequest;
import org.elasticsearch.client.core.CountResponse;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.client.indices.DeleteAliasRequest;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.Operator;
import org.elasticsearch.index.reindex.ReindexRequest;
import org.elasticsearch.repositories.fs.FsRepository;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.util.*;

import static java.util.stream.Collectors.toMap;
import static org.elasticsearch.index.query.QueryBuilders.*;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class DataSearchTest extends CommonTestClass {


    public static final String MOVIE_SEARCH = "movie_search";

    @DisplayName("스냅샷 생성")
    @Test
    void restore_snapshot() throws Exception {

        if (isExistsIndex(MOVIE_SEARCH, testContainerClient)) {
            return;
        }

        //TODO 스냅샷 용 리파지토리 생성
        PutRepositoryRequest putRepositoryRequest = new PutRepositoryRequest();
        Settings.Builder settings = Settings.builder()
                .put("location", "/usr/share/elasticsearch/backup/search")
                .put("compress", true);
        putRepositoryRequest.settings(settings)
                .name("javacafe")
                .type(FsRepository.TYPE);

        AcknowledgedResponse repository = testContainerClient.snapshot().createRepository(putRepositoryRequest, RequestOptions.DEFAULT);
        assertTrue(repository.isAcknowledged());

        GetSnapshotsRequest getSnapshotsRequest = new GetSnapshotsRequest().repository("javacafe");

        GetSnapshotsResponse getSnapshotsResponse = testContainerClient.snapshot().get(getSnapshotsRequest, RequestOptions.DEFAULT);
        System.out.println("getSnapshotsResponse = " + getSnapshotsResponse);


        RestoreSnapshotRequest restoreSnapshotRequest = new RestoreSnapshotRequest()
                .repository("javacafe")
                .snapshot("movie-search");

        RestoreSnapshotResponse restoreSnapshotResponse = testContainerClient.snapshot().restore(restoreSnapshotRequest, RequestOptions.DEFAULT);
        System.out.println("restoreSnapshotResponse = " + restoreSnapshotResponse);
        Thread.sleep(2000);
    }


    @DisplayName("QueryDSL 방식으로 검색")
    @Test
    void search_with_queryDsl() throws Exception {
        restore_snapshot();
        SearchRequest searchRequest = new SearchRequest(MOVIE_SEARCH);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(
                termQuery("prdtYear", "2018")
        );


        searchRequest.source(searchSourceBuilder);

        printSearchResult(searchRequest);


        searchRequest = new SearchRequest(MOVIE_SEARCH);
        searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(
                queryStringQuery("family")
                        .defaultField("movieNmEn")
        );

        searchRequest.source(searchSourceBuilder);

        printSearchResult(searchRequest);
    }

    @DisplayName("복잡한 QueryDSL 작성")
    @Test
    void complex_queryDsl() throws Exception {
        restore_snapshot();
        SearchRequest searchRequest = new SearchRequest(MOVIE_SEARCH);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder
                .query(
                        queryStringQuery("movieNmEn:* OR prdtYear:2017")
                                .defaultField("movieNmEn")
                )
                .from(0)
                .size(5)
                .sort(List.of(
                        SortBuilders.scoreSort().order(SortOrder.DESC),
                        SortBuilders.fieldSort("movieCd").order(SortOrder.ASC)
                ))
                .fetchSource(
                        new String[]{"movieCd", "movieNm", "movieNmEn", "typeNm"},
                        null
                )
        ;

        searchRequest.source(searchSourceBuilder);

        printSearchResult(searchRequest);

    }

    private void printSearchResult(SearchRequest searchRequest) throws IOException {
        SearchResponse searchResponse = testContainerClient.search(searchRequest, RequestOptions.DEFAULT);

        System.err.println("============================= RESULT =============================");
        Arrays.stream(searchResponse.getInternalResponse()
                        .hits()
                        .getHits())
                .map(SearchHit::getSourceAsMap)
                .forEach(System.err::println);
        System.err.println("==================================================================");
    }


    @DisplayName("형태소 분석 수행 후 검색")
    @Test
    void search_with_analyzer() throws Exception {
        restore_snapshot();
        SearchRequest searchRequest = new SearchRequest(MOVIE_SEARCH);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(
                matchQuery("movieNm", "기묘한 가족")
        );
        searchRequest.source(searchSourceBuilder);

        printSearchResult(searchRequest);
    }


    @DisplayName("필터만으로 검색")
    @Test
    void search_with_filter() throws Exception {
        restore_snapshot();
        SearchRequest searchRequest = new SearchRequest(MOVIE_SEARCH);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

        searchSourceBuilder.query(
                boolQuery()
                        .must(
                                matchAllQuery()
                        )
                        .filter(
                                termQuery("repGenreNm", "다큐멘터리")
                        )
        );


        searchRequest.source(searchSourceBuilder);

        printSearchResult(searchRequest);
    }


    @DisplayName("Multi Index 검색")
    @Test
    void search_with_multi_index() throws Exception {
        restore_snapshot();
        if (!isExistsIndex("movie_auto", testContainerClient)) {
            CreateIndexResponse createIndexResponse = testContainerClient.indices().create(new CreateIndexRequest("movie_auto"), RequestOptions.DEFAULT);
            assertTrue(createIndexResponse.isAcknowledged());
        }

        SearchRequest searchRequest = new SearchRequest(MOVIE_SEARCH, "movie_auto");

        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(
                termQuery("repGenreNm", "다큐멘터리")
        );

        searchRequest.source(searchSourceBuilder);

        printSearchResult(searchRequest);
    }


    @DisplayName("페이징 요청")
    @ParameterizedTest(name = "from : {0}")
    @ValueSource(ints = {0, 5, 10, 15})
    void search_with_paging(int from) throws Exception {
        restore_snapshot();
        SearchRequest searchRequest = new SearchRequest(MOVIE_SEARCH);
        searchRequest.source(new SearchSourceBuilder()
                .query(
                        termQuery("repNationNm", "한국")
                )
                .from(from)
                .size(5)
        )
        ;


        printSearchResult(searchRequest);
    }


    @DisplayName("쿼리 결과 정렬")
    @Test
    void search_with_sort() throws Exception {
        restore_snapshot();
        SearchRequest searchRequest = new SearchRequest()
                .indices(MOVIE_SEARCH)
                .source(
                        new SearchSourceBuilder()
                                .query(
                                        termQuery("repNationNm", "한국")
                                )
                                .sort(
                                        List.of(SortBuilders.fieldSort("prdtYear").order(SortOrder.ASC))
                                )
                );

        printSearchResult(searchRequest);
    }


    @DisplayName("추가 정렬 기준")
    @Test
    void search_with_sort_sub() throws Exception {
        restore_snapshot();
        SearchRequest searchRequest = new SearchRequest(MOVIE_SEARCH)
                .source(
                        new SearchSourceBuilder()
                                .query(
                                        termQuery("repNationNm", "한국")
                                )
                                .sort(
                                        List.of(
                                                SortBuilders.fieldSort("prdtYear").order(SortOrder.ASC),
                                                SortBuilders.scoreSort().order(SortOrder.DESC)
                                        )
                                )
                );


        printSearchResult(searchRequest);
    }


    @DisplayName("_source 필드 필터링")
    @Test
    void search_with_source_filtering() throws Exception {
        restore_snapshot();
        SearchRequest searchRequest = new SearchRequest(MOVIE_SEARCH)
                .source(
                        new SearchSourceBuilder()
                                .fetchSource(new String[]{"movieNm"}, null)
                                .query(
                                        termQuery("repNationNm", "한국")
                                )
                );


        printSearchResult(searchRequest);
    }


    @DisplayName("범위 검색")
    @Test
    void search_with_range() throws Exception {
        restore_snapshot();
        SearchRequest searchRequest = new SearchRequest(MOVIE_SEARCH)
                .source(
                        new SearchSourceBuilder()
                                .query(
                                        rangeQuery("prdtYear")
                                                .gte(2016)
                                                .lte(2017)
                                )
                );

        printSearchResult(searchRequest);
    }


    @DisplayName("operator 파라미터 설정")
    @Test
    void search_with_operator() throws Exception {
        restore_snapshot();
        SearchRequest searchRequest = new SearchRequest(MOVIE_SEARCH)
                .source(
                        new SearchSourceBuilder()
                                .query(
                                        matchQuery("movieNm", "자전차왕 엄복동")
                                                //TODO operator 파라미터를 생략하면 OR 연산으로 동작해서
                                                // "자전차왕" 혹은 "엄복동" 이라는 단어가 들어가 있는 모든 문서가 검색될 것이다.
                                                // 하지만 아래에서는 operator 파라미터를 이용해 AND 값을 명시했으므로
                                                // 두 개의 텀("자전차왕", "엄복동") 이 모두 존재하는 문서만 결과로 제공한다.
                                                .operator(Operator.AND)
                                )
                );

        printSearchResult(searchRequest);
    }


    @DisplayName("minimum_should_match 설정 사용")
    @Test
    void search_with_minimum_should_match() throws Exception {
        restore_snapshot();

        SearchRequest searchRequest = new SearchRequest(MOVIE_SEARCH)
                .source(
                        new SearchSourceBuilder()
                                .query(
                                        matchQuery("movieNm", "자전차왕 엄복동")
                                                //TODO 앞서 operator 파라미터를 이용해 AND 연산으로 동작시키는 방법을 살펴보았다.
                                                // 이번에는 OR 연산을 수행할 경우에 사용할 수 있는 옵션을 알아보자.
                                                // 일반적으로 OR 연산을 수행할 경우 검색 결과가 너무 많아질 수 있다.
                                                // 이 경우 텀의 개수가 몇 개 이상 매칭될 때만 검색 결과로 나오게 할 수 있는데
                                                // 이 때 사용하는 파라미터가 minimum_should_match 다.
                                                .minimumShouldMatch("2")
                                        //TODO 위와 같이 작성한다면 텀의 개수와 minimum_should_match 의 개수가 일치하기 때문에 AND 연산과 동일한 효과를 낼 수 있다.
                                )
                );
        printSearchResult(searchRequest);
    }

    //TODO fuzziness 설정
    // fuzziness 파라미터를 사용하면 단순히 같은 값을 찾는 Match Query 를 유사한 값을 찾는 Fuzzy Query 로 변경할 수 있다.
    // 이는 레벤슈타인(Levenshtein) 편집 알고리즘을 기반으로 문서의 필드 값을 여러 번 변경하는 방식으로 동작한다.
    // 유사한 검색 결과를 찾기 위해 허용 범위의 텀으로 변경해 가며 문서를 찾아 결과로 출력한다.
    // 예를 들어 편집 거리의 수를 2로 설정한다면 오차범위가 두 글자 이하인 검색 결과까지 포함해서 결과로 출력한다.
    // 오차범위 값으로 0, 1, 2, AUTO 로 총 4가지 값을 사용할 수 있는데, 이는 알파벳에는 유용하지만 한국어에는 적용하기 어렵다.
    // 하지만 영어를 많이 사용하는 국내 상황에서는 여러 가지 적용 가능한 곳이 있기 때문에 알아두면 유용하디.

    @DisplayName("fuzziness 설정")
    @Test
    void search_with_fuzziness() throws Exception {
        restore_snapshot();
        SearchRequest searchRequest = new SearchRequest(MOVIE_SEARCH)
                .source(
                        new SearchSourceBuilder()
                                .query(
                                        //TODO 예를 들어 아래와 같이 쿼리문에 영화 제목을 실수로 잘못 적을경우에도 검색되게 하고 싶은 경우
                                        // 영화 제목을 Fly High 라고 적어야 하지만 사용자가 Fli High 라고 입력했다면 텀이 일치하지 않기 때문에 검색되지 않는다.
                                        // 하지만 Fuzziness 설정을 사용했다면 검색이 가능해질 것이다.
                                        matchQuery("movieNmEn", "Fli High")
                                                .fuzziness(Fuzziness.ONE)
                                                .operator(Operator.AND)
                                )
                );

        printSearchResult(searchRequest);
    }


    //TODO boost 관련 설정은 검색에서 가장 많이 사용하는 파라미터 중 하나다.
    // 이 파라미터는 관련성이 높은 필드나 키워드에 가중치를 더 줄 수 있게 해준다.
    // 영화 데이터의 경우 한글 영화 제목과 영문 영화 제목 두 가지 제목 필드를 제공하고 있다.
    // 이때 한글 영화 제목에 좀 더 가중치를 부여해서 검색 결과를 좀 더 상위로 올리고 싶을 때 사용할 수 있다.
    @DisplayName("boost 설정")
    @Test
    void search_with_boost() throws Exception {
        restore_snapshot();
        SearchRequest searchRequest = new SearchRequest(MOVIE_SEARCH)
                .source(
                        new SearchSourceBuilder()
                                .query(
                                        multiMatchQuery("Fly", "movieNm", "movieNmEn")
                                                .field("movieNm", 3)
                                )
                );

        printSearchResult(searchRequest);
    }


    @DisplayName("Match All Query")
    @Test
    void search_with_match_all() throws Exception {
        restore_snapshot();
        SearchRequest searchRequest = new SearchRequest(MOVIE_SEARCH)
                .source(
                        new SearchSourceBuilder()
                                .query(
                                        matchAllQuery()
                                )
                );

        printSearchResult(searchRequest);
    }


    @DisplayName("Match Query")
    @Test
    void search_with_match() throws Exception {
        restore_snapshot();
        SearchRequest searchRequest = new SearchRequest(MOVIE_SEARCH)
                .source(
                        new SearchSourceBuilder()
                                .query(
                                        matchQuery("movieNm", "그대 장미")
                                )
                );

        printSearchResult(searchRequest);
    }


    @DisplayName("Multi Match Query")
    @Test
    void search_with_multi_match() throws Exception {
        restore_snapshot();
        SearchRequest searchRequest = new SearchRequest(MOVIE_SEARCH)
                .source(
                        new SearchSourceBuilder()
                                .query(
                                        multiMatchQuery("가족", "movieNm", "movieNmEn")
                                )
                );

        printSearchResult(searchRequest);
    }


    //TODO 텍스트 형태의 값을 검색하기 위해 엘라스틱서치는 두 가지 매핑 유형을 지원한다.
    // Text 데이터 타입 : 필드에 데이터가 저장되기 전에 데이터가 분석되어 역색인 구조로 저장된다.
    // Keyword 데이터 타입 : 데이터가 분석되지 않고 그대로 필드에 저장된다.
    // 이전에 알아본 Match Query(Full Text Query) 는 쿼리를 수행하기 전에 먼저 분석기를 통해 텍스트를 분석한 후 검색을 수행한다.
    // 하지만 Term Query 는 별도의 분석 작업을 수행하지 않고 입력된 텍스트가 존재하는 문서를 찾는다.
    // 따라서 Keyword 데이터 타입을 사용하는 필드를 검색하려면 Term Query 를 사용해야 한다.
    @DisplayName("Term Query")
    @Test
    void search_with_term() throws Exception {
        restore_snapshot();
        SearchRequest searchRequest = new SearchRequest(MOVIE_SEARCH)
                .source(
                        new SearchSourceBuilder()
                                .query(
                                        termQuery("genreAlt", "코미디")
                                )
                );

        printSearchResult(searchRequest);
    }


    @DisplayName("Bool Query")
    @Test
    void search_with_bool_query() throws Exception {
        restore_snapshot();

        //TODO 대표 장르가 "코미디" 이고, 제작 국가에 "한국" 이 포함돼 있으며, 영화 타입 중 "단편" 이 제외된 문서를 검색한다.

        SearchRequest searchRequest = new SearchRequest(MOVIE_SEARCH)
                .source(
                        new SearchSourceBuilder()
                                .query(
                                        boolQuery()
                                                .must(
                                                        termQuery("repGenreNm", "코미디")
                                                )
                                                .must(
                                                        matchQuery("repNationNm", "한국")
                                                )
                                                .mustNot(
                                                        matchQuery("typeNm", "단편")
                                                )
                                )
                );
        printSearchResult(searchRequest);

    }

    //TODO 엘라스틱서치에는 기본적으로 내장된 쿼리 분석기가 있다.
    // query_string 파라미터를 사용하는 쿼리를 작성하면 내장된 쿼리 분석기를 이용하는 질의를 작성할 수 있다.
    @DisplayName("Queyr String")
    @Test
    void search_with_query_string() throws Exception {
        restore_snapshot();
        SearchRequest searchRequest = new SearchRequest(MOVIE_SEARCH)
                .source(
                        new SearchSourceBuilder()
                                .query(
                                        queryStringQuery("(가정) AND (어린이날)")
                                                .defaultField("movieNm")
                                )
                );

        printSearchResult(searchRequest);
    }

    //TODO Prefix Query 는 해당 접두어가 있는 모든 문서를 검색하는데 사용한다.
    // 역색인된 텀은 사전순으로 정렬되고 Prefix Query 는 저장된 텀들을 스캔해서 일차하는 텀을 찾는다.
    @DisplayName("Prefix Query")
    @Test
    void search_with_prefix_query() throws Exception {
        restore_snapshot();
        SearchRequest searchRequest = new SearchRequest(MOVIE_SEARCH)
                .source(
                        new SearchSourceBuilder()
                                .query(
                                        prefixQuery("movieNm", "자전차")
                                )
                );

        printSearchResult(searchRequest);
    }

    //TODO 문서를 색인할 때 필드의 값이 없다면 필드를 생성하지 않거나 필드의 값을 null 로 설정할 때가 있다.
    // 이러한 데이터를 제외하고 실제 값이 존재하는 문서만 찾고 싶다면 Exists Query 를 사용하면 된다.
    @DisplayName("Exists Query")
    @Test
    void search_with_exists_query() throws Exception {
        restore_snapshot();
        SearchRequest searchRequest = new SearchRequest(MOVIE_SEARCH)
                .source(
                        new SearchSourceBuilder()
                                .query(
                                        existsQuery("movieNm")
                                )
                );

        printSearchResult(searchRequest);
    }

    //TODO WildCard  검색어가 와일드카드와 일치하는 구문을 찾는다. 이 때 입력된 검색어는 형태소 분석이 이뤄지지 않는다.
    // 와일드카드 옵션은 두 가지를 선택적으로 사용할 수 있다. 와일드카드를 사용할 경우 단어의 첫 글자로는 절대 사용해서는 안된다.
    // 첫 글자로 와일드카드가 사용될 경우 색인된 전체 문서를 찾아야 하는 불상사가 발생할 수 있기 때문이다.
    @DisplayName("Wildcard Query")
    @Test
    void search_with_wildcard_query() throws Exception {
        restore_snapshot();
        SearchRequest searchRequest = new SearchRequest(MOVIE_SEARCH)
                .source(
                        new SearchSourceBuilder()
                                .query(
                                        wildcardQuery("typeNm", "장?")
                                )
                );

        printSearchResult(searchRequest);
    }

    @DisplayName("Nested 쿼리")
    @Test
    void search_with_nested_query() throws Exception {
        CreateIndexRequest createIndexRequest = new CreateIndexRequest("movie_nested");
        XContentBuilder settingBuilder = XContentFactory.jsonBuilder();
        settingBuilder.startObject();
        {
            settingBuilder.field("number_of_replicas", 1);
            settingBuilder.field("number_of_shards", 5);
        }
        settingBuilder.endObject();
        createIndexRequest.settings(settingBuilder);


        XContentBuilder mappingBuilder = XContentFactory.jsonBuilder();
        mappingBuilder.startObject();
        {
            mappingBuilder.startObject("properties");
            {
                mappingBuilder.startObject("repGenreNm");
                {
                    mappingBuilder.field("type", "keyword");
                }
                mappingBuilder.endObject();
                mappingBuilder.startObject("companies");
                {
                    mappingBuilder.field("type", "nested");
                    mappingBuilder.startObject("properties");
                    {
                        mappingBuilder.startObject("companyCd");
                        {
                            mappingBuilder.field("type", "keyword");
                        }
                        mappingBuilder.endObject();
                        mappingBuilder.startObject("companyNm");
                        {
                            mappingBuilder.field("type", "text");
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

        createIndexRequest.mapping(mappingBuilder);

        CreateIndexResponse createIndexResponse = testContainerClient.indices().create(createIndexRequest, RequestOptions.DEFAULT);
        assertCreatedIndex(createIndexResponse, "movie_nested");

        //TODO 생성된 인덱스에 문서 추가하기
        Map<String, Object> source = new HashMap<>();

        source.put("movieCd", "20184623");
        source.put("movieNm", "바람난 아내들2");
        source.put("movieNmEn", "");
        source.put("prdtYear", "2018");
        source.put("openDt", "");
        source.put("typeNm", "장편");
        source.put("prdtStatNm", "개봉예정");
        source.put("nationAlt", "한국");
        source.put("genreAlt", "멜로/로맨스");
        source.put("repNationNm", "한국");
        source.put("repGenreNm", "멜로/로맨스");
        source.put("companies", new Object[]{
                Map.of("companyCd", "20173401"),
                Map.of("companyNm", "(주)케이피에이기획")
        });

        IndexRequest indexRequest = new IndexRequest("movie_nested")
                .source(source);


        IndexResponse indexResponse = testContainerClient.index(indexRequest, RequestOptions.DEFAULT);
        System.out.println("indexResponse = " + indexResponse);
        Thread.sleep(2000);

        //TODO NestedQuery 를 이용해 Child 로 저장된 문서의 특정 필드를 검색할 수 있다.
        SearchRequest searchRequest = new SearchRequest("movie_nested")
                .source(
                        new SearchSourceBuilder()
                                .query(
                                        boolQuery()
                                                .must(
                                                        termQuery("repGenreNm", "멜로/로맨스")
                                                )
                                                .must(
                                                        nestedQuery("companies", boolQuery()
                                                                .filter(
                                                                        termQuery("companies.companyCd", "20173401")
                                                                ), ScoreMode.None)
                                                )
                                )
                );

        printSearchResult(searchRequest);


    }

    @DisplayName("동적으로 요청을 분배하도록 설정")
    @Test
    void cluster_settings_with_route() throws Exception {
        Settings.Builder setting = Settings.builder()
                .put("cluster.routing.use_adaptive_replica_selection", true);
        ClusterUpdateSettingsRequest settingRequest = new ClusterUpdateSettingsRequest()
                .transientSettings(setting);

        ClusterUpdateSettingsResponse settingResponse = testContainerClient.cluster().putSettings(settingRequest, RequestOptions.DEFAULT);
        assertTrue(settingResponse.isAcknowledged());
    }

    @DisplayName("글로벌 타임아웃 설정")
    @Test
    void cluster_setting_with_timeout() throws Exception {
        //TODO 글로벌로 적용되는 타임아웃의 기본 정책은 무제한(-1) 이다.
        Settings.Builder settings = Settings.builder()
                .put("search.default_search_timeout", "1s");
        ClusterUpdateSettingsRequest settingsRequest = new ClusterUpdateSettingsRequest()
                .transientSettings(settings);


        ClusterUpdateSettingsResponse settingsResponse = testContainerClient.cluster().putSettings(settingsRequest, RequestOptions.DEFAULT);
        assertTrue(settingsResponse.isAcknowledged());
    }


    //TODO Multi Search API 를 사용하면 동시에 여러 개의 색인에서 검색을 수행할 수 있으므로 사용자별 맞춤 페이지 등을 구현할 때 여러 인덱스에서
    // 사용자별로 특화된 정보를 가져오거나 할 때 유용하게 활용할 수 있다.
    @DisplayName("Multi Search API")
    @Test
    void search_with_multi_search() throws Exception {
        restore_snapshot();
        MultiSearchRequest multiSearchRequest = new MultiSearchRequest()
                .add(
                        new SearchRequest(MOVIE_SEARCH)
                                .source(
                                        new SearchSourceBuilder()
                                                .query(
                                                        matchAllQuery()
                                                )
                                                .size(10)
                                )
                )
                .add(
                        new SearchRequest("movie_auto")
                                .source(
                                        new SearchSourceBuilder()
                                                .query(
                                                        matchAllQuery()
                                                )
                                                .size(10)
                                )
                );

        MultiSearchResponse multiSearchResponse = testContainerClient.msearch(multiSearchRequest, RequestOptions.DEFAULT);
        System.out.println("multiSearchResponse = " + multiSearchResponse);
        Arrays.stream(multiSearchResponse
                        .getResponses())
                .map(MultiSearchResponse.Item::getResponse)
                .filter(Objects::nonNull)
                .map(SearchResponse::getInternalResponse)
                .map(SearchResponseSections::hits)
                .map(SearchHits::getHits)
                .flatMap(Arrays::stream)
                .map(SearchHit::getSourceAsMap)
                .forEach(System.err::println);
    }

    @DisplayName("Count API")
    @Test
    void search_with_count() throws Exception {
        restore_snapshot();
        CountRequest countRequest = new CountRequest(MOVIE_SEARCH)
                .query(
                        queryStringQuery("2017")
                                .defaultField("prdtYear")
                );

        CountResponse countResponse = testContainerClient.count(countRequest, RequestOptions.DEFAULT);
        System.out.println("countResponse = " + countResponse);
    }


    @DisplayName("Validate API")
    @Test
    void use_validate_api() throws Exception {
        //TODO Validate API 를 사용하면 쿼리를 실행하기에 앞서 쿼리가 유효하게 작성됐는지 검증하는 것이 가능하다.
        restore_snapshot();
        ValidateQueryRequest validateQueryRequest = new ValidateQueryRequest(MOVIE_SEARCH)
                .query(
                        matchQuery("prdtYear", 2017)
                );

        ValidateQueryResponse validateQueryResponse = testContainerClient.indices().validateQuery(validateQueryRequest, RequestOptions.DEFAULT);
        assertTrue(validateQueryResponse.isValid());
    }

    @DisplayName("Explain API")
    @Test
    void use_explain_api() throws Exception {
        restore_snapshot();
        SearchRequest searchRequest = new SearchRequest(MOVIE_SEARCH)
                .source(
                        new SearchSourceBuilder()
                                .query(
                                        termQuery("movieCd", "20178401")
                                )
                );

        SearchResponse searchResponse = testContainerClient.search(searchRequest, RequestOptions.DEFAULT);
        System.out.println("searchResponse = " + searchResponse);

        //TODO 방금 조회한 ID가 "yz3JqmkBjjM-ebDb8AUS" 인 문서에 대해 스코어 값이 어떤 방식으로 계산됐는지 알아보자.

        ExplainRequest explainRequest = new ExplainRequest(MOVIE_SEARCH, "yz3JqmkBjjM-ebDb8AUS")
                .query(
                        termQuery("movieCd", "20178401")
                );

        ExplainResponse explainResponse = testContainerClient.explain(explainRequest, RequestOptions.DEFAULT);
        System.err.println(explainResponse.getExplanation());
    }

    //TODO Profile API 는 쿼리에 대한 사\ㅇ세한 수행 계획과 각 수행 계획별로 수행된 시간을 돌려주므로
    // 성능을 튜닝하거나 디버깅할 때 유용하게 활용할 수 있다. 다만 사용할 때 반드시 확인해야 하는 점은
    // Profile API 는 쿼리에 대한 내용을 매우 상세하게 설명하므로 결과가 매우 방대하다는 점이다.
    // 특히 여러 샤드에 걸쳐 검색되는 쿼리의 경우에는 더욱더 결괏값이 장황하게 길어지기 때문에 확인하기가 매우 어렵다.
    @DisplayName("Profile API")
    @Test
    void use_profile_api() throws Exception {
        restore_snapshot();
        SearchRequest searchRequest = new SearchRequest(MOVIE_SEARCH)
                .source(
                        new SearchSourceBuilder()
                                .profile(true)
                                .query(
                                        matchAllQuery()
                                )
                );


        SearchResponse searchResponse = testContainerClient.search(searchRequest, RequestOptions.DEFAULT);
        searchResponse
                .getProfileResults()
                .entrySet()
                .stream()
                .collect(toMap(Map.Entry::getKey,
                        e -> e.getValue().getQueryProfileResults().get(0).getQueryResults().get(0).getTime()))
                .entrySet()
                .forEach(System.err::println);
    }


    @DisplayName("reindex API 를 사용하 movie_info 인덱스 생성")
    @Test
    void reindex_movie_info_index() throws Exception {
        //TODO 이미 만들어진 Aliase 가 있다면 삭제 후 재생성
        GetAliasesRequest getAliasesRequest = new GetAliasesRequest("movie");
        boolean isExists = testContainerClient.indices().existsAlias(getAliasesRequest, RequestOptions.DEFAULT);
        if (isExists) {
            DeleteAliasRequest deleteAliasRequest = new DeleteAliasRequest(MOVIE_SEARCH, "movie");
            org.elasticsearch.client.core.AcknowledgedResponse deleteAliasResponse = testContainerClient.indices().deleteAlias(deleteAliasRequest, RequestOptions.DEFAULT);
            assertTrue(deleteAliasResponse.isAcknowledged());
        }

        restore_snapshot();
        ReindexRequest reindexRequest = new ReindexRequest()
                .setSourceIndices(MOVIE_SEARCH)
                .setDestIndex("movie_info");

        testContainerClient.reindex(reindexRequest, RequestOptions.DEFAULT);

        Thread.sleep(2000);

        IndicesAliasesRequest indicesAliasesRequest = new IndicesAliasesRequest();
        IndicesAliasesRequest.AliasActions movieSearchAlias = new IndicesAliasesRequest.AliasActions(IndicesAliasesRequest.AliasActions.Type.ADD)
                .index(MOVIE_SEARCH)
                .alias("movie");


        IndicesAliasesRequest.AliasActions movieInfoAlias = new IndicesAliasesRequest.AliasActions(IndicesAliasesRequest.AliasActions.Type.ADD)
                .index("movie_info")
                .alias("movie");


        indicesAliasesRequest.addAliasAction(movieSearchAlias)
                .addAliasAction(movieInfoAlias);

        AcknowledgedResponse acknowledgedResponse = testContainerClient.indices().updateAliases(indicesAliasesRequest, RequestOptions.DEFAULT);
        assertTrue(acknowledgedResponse.isAcknowledged());


        SearchRequest searchRequest = new SearchRequest("movie");
        SearchResponse searchResponse = testContainerClient.search(searchRequest, RequestOptions.DEFAULT);
        System.out.println("searchResponse = " + searchResponse);
    }

}
