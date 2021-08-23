package com.gravylab.elasticsearchguide;

import org.elasticsearch.action.admin.cluster.repositories.put.PutRepositoryRequest;
import org.elasticsearch.action.admin.cluster.snapshots.get.GetSnapshotsRequest;
import org.elasticsearch.action.admin.cluster.snapshots.get.GetSnapshotsResponse;
import org.elasticsearch.action.admin.cluster.snapshots.restore.RestoreSnapshotRequest;
import org.elasticsearch.action.admin.cluster.snapshots.restore.RestoreSnapshotResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.repositories.fs.FsRepository;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.bucket.MultiBucketsAggregation;
import org.elasticsearch.search.aggregations.bucket.range.ParsedDateRange;
import org.elasticsearch.search.aggregations.bucket.range.ParsedRange;
import org.elasticsearch.search.aggregations.bucket.terms.ParsedStringTerms;
import org.elasticsearch.search.aggregations.metrics.*;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;
import static org.elasticsearch.index.query.QueryBuilders.*;
import static org.elasticsearch.search.aggregations.AggregationBuilders.*;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class AggregationTest extends CommonTestClass {

    public static final String APACHE_WEB_LOG = "apache-web-log";
    public static final String APACHE_WEB_LOG_APPLIED_MAPPING = APACHE_WEB_LOG + "-applied-mapping";

    @DisplayName("스냅숏 목록 확인")
    @Test
    void restore_snapshot() throws Exception {

        if (isExistsIndex(APACHE_WEB_LOG, testContainerClient)) {
            return;
        }
        PutRepositoryRequest putRepositoryRequest = new PutRepositoryRequest();
        Settings.Builder builder = Settings.builder();
        Settings.Builder settings = builder.put("location", "/usr/share/elasticsearch/backup/agg")
                .put("compress", true);
        putRepositoryRequest.settings(settings)
                .name(APACHE_WEB_LOG)
                .type(FsRepository.TYPE);


        AcknowledgedResponse putRepositoryResponse = testContainerClient.snapshot().createRepository(putRepositoryRequest, RequestOptions.DEFAULT);
        assertTrue(putRepositoryResponse.isAcknowledged());


        //TODO 스냅숏 목록 확인
        GetSnapshotsRequest getSnapshotsRequest = new GetSnapshotsRequest().repository(APACHE_WEB_LOG);
        GetSnapshotsResponse getSnapshotsResponse = testContainerClient.snapshot().get(getSnapshotsRequest, RequestOptions.DEFAULT);
        getSnapshotsResponse.getSnapshots()
                .forEach(System.err::println);

        RestoreSnapshotRequest restoreSnapshotRequest = new RestoreSnapshotRequest()
                .repository(APACHE_WEB_LOG)
                .snapshot("default");

        RestoreSnapshotResponse restoreSnapshotsResponse = testContainerClient.snapshot().restore(restoreSnapshotRequest, RequestOptions.DEFAULT);
        System.out.println("restoreSnapshotsResponse = " + restoreSnapshotsResponse);
        Thread.sleep(2000);
    }


    @DisplayName("지역별 사용자의 접속 수")
    @Test
    void aggregation_by_location_user() throws Exception {
        restore_snapshot();
        SearchRequest searchRequest = new SearchRequest(APACHE_WEB_LOG)
                .source(
                        new SearchSourceBuilder()
                                .query(
                                        matchAllQuery()
                                )
                                .aggregation(
                                        terms("region_count")
                                                .field("geoip.region_name.keyword")
                                                .size(20)
                                )
                );


        SearchResponse searchResponse = testContainerClient.search(searchRequest, RequestOptions.DEFAULT);
        searchResponse
                .getAggregations()
                .asList()
                .stream().map(e -> ((ParsedStringTerms) e).getBuckets())
                .collect(toList())
                .get(0)
                .stream()
                .collect(toMap(MultiBucketsAggregation.Bucket::getKey, MultiBucketsAggregation.Bucket::getDocCount))
                .entrySet()
                .forEach(System.err::println);
    }


    @DisplayName("합산 집계")
    @Test
    void aggregation_with_sum() throws Exception {
        restore_snapshot();
        SearchRequest searchRequest = new SearchRequest(APACHE_WEB_LOG)
                .source(
                        new SearchSourceBuilder()
                                .size(0)
                                .aggregation(
                                        sum("total_bytes")
                                                .field("bytes")
                                )
                );

        printAggregation(searchRequest, "total_bytes");
    }

    @DisplayName("filter 기능을 사용해 특정 지역에서 유입된 데이터의 합을 계산해보기")
    @Test
    void aggregation_with_sum_by_filter() throws Exception {
        restore_snapshot();
        SearchRequest searchRequest = new SearchRequest(APACHE_WEB_LOG)
                .source(
                        new SearchSourceBuilder()
                                .query(
                                        constantScoreQuery(
                                                matchQuery("geoip.city_name", "Paris")
                                        )
                                )
                                .aggregation(
                                        sum("total_bytes")
                                                .field("bytes")
                                                .field("bytes")
                                )
                );


        printAggregation(searchRequest, "total_bytes");
    }


    @DisplayName("Script 기능을 사용하여 합 연산 수행하기")
    @Test
    void aggregation_with_script() throws Exception {
        restore_snapshot();
        SearchRequest searchRequest = new SearchRequest(APACHE_WEB_LOG)
                .source(
                        new SearchSourceBuilder()
                                .query(
                                        constantScoreQuery(
                                                matchQuery("geoip.city_name", "Paris")
                                        )
                                )
                                .aggregation(
                                        sum("total_bytes")
                                                .script(
                                                        createScript("doc.bytes.value", new HashMap<>())
                                                )
                                )
                );

        printAggregation(searchRequest, "total_bytes");

    }

    @DisplayName("Script 기능을 사용하여 더 복잡한 연산 수행하기")
    @Test
    void aggregation_with_complex_script() throws Exception {
        restore_snapshot();
        SearchRequest searchRequest = new SearchRequest(APACHE_WEB_LOG)
                .source(
                        new SearchSourceBuilder()
                                .query(
                                        constantScoreQuery(
                                                matchQuery("geoip.city_name", "Paris")
                                        )
                                )
                                .aggregation(
                                        sum("total_bytes")
                                                .script(
                                                        createScript("doc.bytes.value / (double)params.divide_value", Map.of("divide_value", 1000))
                                                )
                                )
                );

        printAggregation(searchRequest, "total_bytes");
    }


    @DisplayName("평균 집계")
    @Test
    void aggregation_with_avg() throws Exception {
        restore_snapshot();
        SearchRequest searchRequest = new SearchRequest(APACHE_WEB_LOG)
                .source(
                        new SearchSourceBuilder()
                                .size(0)
                                .aggregation(
                                        avg("avg_bytes")
                                                .field("bytes")
                                )
                );

        printAggregation(searchRequest, "avg_bytes");
    }

    @DisplayName("Filter 기능을 사용해 특정 지역에서 유입된 데이터의 평균을 계산")
    @Test
    void aggregation_with_avg_by_filter() throws Exception {
        restore_snapshot();
        SearchRequest searchRequest = new SearchRequest(APACHE_WEB_LOG)
                .source(
                        new SearchSourceBuilder()
                                .query(
                                        constantScoreQuery(
                                                matchQuery("geoip.city_name", "Paris")
                                        )
                                )
                                .aggregation(
                                        avg("avg_bytes")
                                                .field("bytes")
                                )
                );


        printAggregation(searchRequest, "avg_bytes");
    }


    @DisplayName("최솟값 집계")
    @Test
    void aggregation_with_min() throws Exception {
        restore_snapshot();
        SearchRequest searchRequest = new SearchRequest(APACHE_WEB_LOG)
                .source(
                        new SearchSourceBuilder()
                                .size(0)
                                .aggregation(
                                        min("min_bytes")
                                                .field("bytes")
                                )
                );
        printAggregation(searchRequest, "min_bytes");
    }


    @DisplayName("Filter 기능을 사용해 특정 지역에서 유입된 데이터의 가장 작은 값")
    @Test
    void aggregation_with_min_by_filter() throws Exception {
        restore_snapshot();
        SearchRequest searchRequest = new SearchRequest(APACHE_WEB_LOG)
                .source(
                        new SearchSourceBuilder()
                                .query(
                                        constantScoreQuery(
                                                matchQuery("geoip.city_name", "Paris")
                                        )
                                )
                                .aggregation(
                                        min("min_bytes")
                                                .field("bytes")
                                )
                );

        printAggregation(searchRequest, "min_bytes");
    }


    @DisplayName("최댓값 집계")
    @Test
    void aggregation_with_max() throws Exception {
        restore_snapshot();
        SearchRequest searchRequest = new SearchRequest(APACHE_WEB_LOG)
                .source(
                        new SearchSourceBuilder()
                                .size(0)
                                .aggregation(
                                        max("max_bytes")
                                                .field("bytes")
                                )
                );

        printAggregation(searchRequest, "max_bytes");

    }


    @DisplayName("Filter 기능을 사용해 특정 지역에서 유입된 데이터의 가장 큰 값")
    @Test
    void aggregation_with_max_by_filter() throws Exception {
        restore_snapshot();
        SearchRequest searchRequest = new SearchRequest(APACHE_WEB_LOG)
                .source(
                        new SearchSourceBuilder()
                                .query(
                                        constantScoreQuery(
                                                matchQuery("geoip.city_name", "Paris")
                                        )
                                )
                                .aggregation(
                                        max("max_bytes")
                                                .field("bytes")
                                )
                );
        printAggregation(searchRequest, "max_bytes");

    }

    @DisplayName("개수 집계")
    @Test
    void aggregation_with_value_count() throws Exception {
        restore_snapshot();
        SearchRequest searchRequest = new SearchRequest(APACHE_WEB_LOG)
                .source(
                        new SearchSourceBuilder()
                                .size(0)
                                .aggregation(
                                        count("bytes_count")
                                                .field("bytes")
                                )
                );

        printAggregation(searchRequest, "bytes_count");
    }

    @DisplayName("Filter 기능을 사용해 특정 지역에서 일어난 사용자 요청 횟수")
    @Test
    void aggregation_with_value_count_by_filter() throws Exception {
        restore_snapshot();
        SearchRequest searchRequest = new SearchRequest(APACHE_WEB_LOG)
                .source(
                        new SearchSourceBuilder()
                                .query(
                                        constantScoreQuery(
                                                matchQuery("geoip.city_name", "Paris")
                                        )
                                )
                                .aggregation(
                                        count("bytes_count")
                                                .field("bytes")
                                )
                );

        printAggregation(searchRequest, "bytes_count");
    }


    @DisplayName("통계 집계")
    @Test
    void aggregation_with_stats() throws Exception {
        restore_snapshot();
        SearchRequest searchRequest = new SearchRequest(APACHE_WEB_LOG)
                .source(
                        new SearchSourceBuilder()
                                .aggregation(
                                        stats("bytes_stats")
                                                .field("bytes")
                                )
                );

        printAggregation(searchRequest, "bytes_stats");
    }


    @DisplayName("Filter 기능을 사용해 특정 지역에 대한 통계 집계")
    @Test
    void aggregation_with_stats_by_filter() throws Exception {
        restore_snapshot();
        SearchRequest searchRequest = new SearchRequest(APACHE_WEB_LOG)
                .source(
                        new SearchSourceBuilder()
                                .query(
                                        constantScoreQuery(
                                                matchQuery("geoip.city_name", "Paris")
                                        )
                                )
                                .aggregation(
                                        stats("bytes_stats")
                                                .field("bytes")
                                )
                );

        printAggregation(searchRequest, "bytes_stats");
    }


    //TODO 확장 통계 집계(Extended Stats Aggregation)는 결과값이 여러 개인 다중 숫자 메틕 집계에 해당한다.
    // 앞서 살펴본 통계 집계를 확장해서 표준편차 같은 통계값이 추가됫다.
    @DisplayName("확장 통계 집계")
    @Test
    void aggregation_with_extended_stats() throws Exception {
        restore_snapshot();
        SearchRequest searchRequest = new SearchRequest(APACHE_WEB_LOG)
                .source(
                        new SearchSourceBuilder()
                                .aggregation(
                                        extendedStats("bytes_extended_stats")
                                                .field("bytes")
                                )
                );

        printAggregation(searchRequest, "bytes_extended_stats");
    }

    @DisplayName("Filter 기능을 사용해 특정 지역에 대한 확장 통계 집계")
    @Test
    void aggregation_with_extended_stats_by_filter() throws Exception {
        restore_snapshot();
        SearchRequest searchRequest = new SearchRequest(APACHE_WEB_LOG)
                .source(
                        new SearchSourceBuilder()
                                .query(
                                        constantScoreQuery(
                                                matchQuery("geoip.city_name", "Paris")
                                        )
                                )
                                .aggregation(
                                        extendedStats("bytes_extended_stats")
                                                .field("bytes")
                                )
                );

        printAggregation(searchRequest, "bytes_extended_stats");
    }


    @DisplayName("terms 집계를 통해 미국의 어느 지역에서 데이터 유입이 있었는지 확인하기")
    @Test
    void aggregation_with_terms() throws Exception {
        restore_snapshot();
        SearchRequest searchRequest = new SearchRequest(APACHE_WEB_LOG)
                .source(
                        new SearchSourceBuilder()
                                .size(0)
                                .query(
                                        constantScoreQuery(
                                                matchQuery("geoip.country_name", "United States")
                                        )
                                )
                                .aggregation(
                                        terms("us_city_names")
                                                .field("geoip.city_name.keyword")
                                )
                );


        printAggregation(searchRequest, "us_city_names");
    }

    @DisplayName("카디널리티 집계")
    @Test
    void aggregation_with_cardinality() throws Exception {
        restore_snapshot();
        SearchRequest searchRequest = new SearchRequest(APACHE_WEB_LOG)
                .source(
                        new SearchSourceBuilder()
                                .size(0)
                                .query(
                                        constantScoreQuery(
                                                matchQuery("geoip.country_name", "United States")
                                        )
                                )
                                .aggregation(
                                        cardinality("us_cardinality")
                                                .field("geoip.city_name.keyword")
                                )
                );

        printAggregation(searchRequest, "us_cardinality");
    }


    @DisplayName("백분위 수 집계")
    @Test
    void aggregation_with_percentiles() throws Exception {
        restore_snapshot();
        SearchRequest searchRequest = new SearchRequest(APACHE_WEB_LOG)
                .source(
                        new SearchSourceBuilder()
                                .size(0)
                                .aggregation(
                                        percentiles("bytes_percentiles")
                                                .field("bytes")
                                )
                );

        printAggregation(searchRequest, "bytes_percentiles");
    }

    @DisplayName("백분위 수 랭크 집계")
    @Test
    void aggregation_with_percentile_ranks() throws Exception {
        restore_snapshot();
        double[] doubles = {5000.0, 10000.0};
        SearchRequest searchRequest = new SearchRequest(APACHE_WEB_LOG)
                .source(
                        new SearchSourceBuilder()
                                .size(0)
                                .aggregation(
                                        percentileRanks("bytes_percentiles_ranks", doubles)
                                                .field("bytes")
                                )
                );

        printAggregation(searchRequest, "bytes_percentiles_ranks");
    }

    @DisplayName("지형 경계 집계를 위한 스냅숏 복원")
    @Test
    void restore_snapshot_for_geo_bounds_aggregation() throws Exception {
        if (isExistsIndex(APACHE_WEB_LOG_APPLIED_MAPPING, testContainerClient)) {
            return;
        }
        PutRepositoryRequest putRepositoryRequest = new PutRepositoryRequest();
        Settings.Builder builder = Settings.builder();
        Settings.Builder settings = builder.put("location", "/usr/share/elasticsearch/backup/agg")
                .put("compress", true);
        putRepositoryRequest.settings(settings)
                .name(APACHE_WEB_LOG)
                .type(FsRepository.TYPE);


        AcknowledgedResponse putRepositoryResponse = testContainerClient.snapshot().createRepository(putRepositoryRequest, RequestOptions.DEFAULT);
        assertTrue(putRepositoryResponse.isAcknowledged());


        //TODO 스냅숏 목록 확인
        GetSnapshotsRequest getSnapshotsRequest = new GetSnapshotsRequest().repository(APACHE_WEB_LOG);
        GetSnapshotsResponse getSnapshotsResponse = testContainerClient.snapshot().get(getSnapshotsRequest, RequestOptions.DEFAULT);
        getSnapshotsResponse.getSnapshots()
                .forEach(System.err::println);

        RestoreSnapshotRequest restoreSnapshotRequest = new RestoreSnapshotRequest()
                .repository(APACHE_WEB_LOG)
                .snapshot("applied-mapping");

        RestoreSnapshotResponse restoreSnapshotsResponse = testContainerClient.snapshot().restore(restoreSnapshotRequest, RequestOptions.DEFAULT);
        System.out.println("restoreSnapshotsResponse = " + restoreSnapshotsResponse);
        Thread.sleep(2000);
    }

    @DisplayName("수집된 모든 데이터에 대한 지형 경계를 집계")
    @Test
    void aggregation_with_geo_bounds() throws Exception {
        restore_snapshot_for_geo_bounds_aggregation();
        SearchRequest searchRequest = new SearchRequest(APACHE_WEB_LOG_APPLIED_MAPPING)
                .source(
                        new SearchSourceBuilder()
                                .size(0)
                                .aggregation(
                                        geoBounds("viewport")
                                                .field("geoip.location")
                                                .wrapLongitude(true)
                                )
                );

        printAggregation(searchRequest, "viewport");
    }


    //TODO 지형 경계 집계(Geo Bounds Aggregation) 은 지형 좌표를 포함하고 있는 필드에 대해 해당 지역 경계 상자를 계산하는 메트릭 집계다.
    // 이 집계를 사용하려면 계산하려는 필드의 타입이 geo_point 여야 한다.
    @DisplayName("Filter 기능을 사용해 특정 지형의 경계 집계")
    @Test
    void aggregation_with_geo_bounds_by_filter() throws Exception {
        restore_snapshot_for_geo_bounds_aggregation();
        SearchRequest searchRequest = new SearchRequest(APACHE_WEB_LOG_APPLIED_MAPPING)
                .source(
                        new SearchSourceBuilder()
                                .size(0)
                                .query(
                                        constantScoreQuery(
                                                matchQuery("geoip.continent_code", " EU")
                                        )
                                )
                                .aggregation(
                                        geoBounds("viewport")
                                                .field("geoip.location")
                                                .wrapLongitude(true)
                                )
                );

        printAggregation(searchRequest, "viewport");
    }


    //TODO 지형 중심 집계(Geo Centroid Aggregation) 는 앞서 살펴본 지형 경계 집계의 범위에서 정가운데의 위치를 반환한다.
    @DisplayName("지형 중심 집계")
    @Test
    void aggregation_with_geo_centroid() throws Exception {
        restore_snapshot_for_geo_bounds_aggregation();
        SearchRequest searchRequest = new SearchRequest(APACHE_WEB_LOG_APPLIED_MAPPING)
                .source(
                        new SearchSourceBuilder()
                                .size(0)
                                .aggregation(
                                        geoCentroid("centroid")
                                                .field("geoip.location")
                                )
                );

        printAggregation(searchRequest, "centroid");
    }


    @DisplayName("범위 집계")
    @Test
    void aggregation_with_range() throws Exception {
        restore_snapshot();
        SearchRequest searchRequest = new SearchRequest(APACHE_WEB_LOG)
                .source(
                        new SearchSourceBuilder()
                                .size(0)
                                .aggregation(
                                        range("bytes_range")
                                                .field("bytes")
                                                .addRange(1000, 2000)
                                )
                );

        printAggregation(searchRequest, "bytes_range");
    }

    @DisplayName("범위 집계 여러개 지정")
    @Test
    void aggregation_with_multi_range() throws Exception {
        restore_snapshot();
        SearchRequest searchRequest = new SearchRequest(APACHE_WEB_LOG)
                .source(
                        new SearchSourceBuilder()
                                .size(0)
                                .aggregation(
                                        range("bytes_range")
                                                .field("bytes")
                                                .addRange(0, 1000)
                                                .addRange(1000, 2000)
                                                .addRange(2000, 3000)
                                )
                );

        printAggregation(searchRequest, "bytes_range");
    }

    @DisplayName("범위 집계 key 설정")
    @Test
    void aggregation_with_multi_range_with_key_setting() throws Exception {
        restore_snapshot();
        SearchRequest searchRequest = new SearchRequest(APACHE_WEB_LOG)
                .source(
                        new SearchSourceBuilder()
                                .size(0)
                                .aggregation(
                                        range("bytes_range")
                                                .field("bytes")
                                                .addRange("small", Double.NEGATIVE_INFINITY, 1000)
                                                .addRange("medium", 1000, 2000)
                                                .addRange("large", 2000, 3000)
                                )
                );

        printAggregation(searchRequest, "bytes_range");
    }


    @DisplayName("날짜 범위 집계")
    @Test
    void aggregation_with_date_range() throws Exception {
        restore_snapshot();
        SearchRequest searchRequest = new SearchRequest(APACHE_WEB_LOG)
                .source(
                        new SearchSourceBuilder()
                                .size(0)
                                .aggregation(
                                        dateRange("request count with date range")
                                                .field("timestamp")
                                                .addRange("2015-01-04T05:14:00.000Z", "2015-01-04T05:16:00.000Z")
                                )
                );

        printAggregation(searchRequest, "request count with date range");
    }


    private Script createScript(String script, Map<String, Object> params) {
        return new Script(ScriptType.INLINE, "painless", script, params);
    }

    private void printAggregation(SearchRequest searchRequest, String aggregationName) throws IOException {
        SearchResponse searchResponse = testContainerClient.search(searchRequest, RequestOptions.DEFAULT);


        Aggregation aggregation = searchResponse
                .getAggregations()
                .get(aggregationName);

        String className = aggregation.getClass().getSimpleName();
        switch (className) {
            case "ParsedStats":
                printParsedStats(aggregation);
                break;
            case "ParsedExtendedStats":
                printParsedExtendedStats(aggregation);
                break;
            case "ParsedStringTerms":
                printParsedStringTerms(aggregation);
                break;
            case "ParsedCardinality":
                printParsedCardinality(aggregation);
                break;
            case "ParsedTDigestPercentiles":
                printParsedTDigestPercentiles(aggregation);
                break;
            case "ParsedTDigestPercentileRanks":
                printParsedTDigestPercentileRanks(aggregation);
                break;
            case "ParsedGeoBounds":
                printParsedGeoBounds(aggregation);
                break;
            case "ParsedGeoCentroid":
                printParsedGeoCentroid(aggregation);
                break;
            case "ParsedRange":
                printParsedRange(aggregation);
                break;
            case "ParsedDateRange":
                printParsedDateRange(aggregation);
                break;
            default:
                printSingleValue(aggregation);
                break;
        }


    }

    private void printParsedDateRange(Aggregation aggregation) {
        ((ParsedDateRange) aggregation).getBuckets()
                .listIterator()
                .forEachRemaining(e -> {
                    Object key = e.getKey();
                    Object from = e.getFrom();
                    Object to = e.getTo();
                    long docCount = e.getDocCount();

                    System.err.println("key = " + key);
                    System.err.println("from = " + from);
                    System.err.println("to = " + to);
                    System.err.println("docCount = " + docCount);

                });

    }

    private void printParsedRange(Aggregation aggregation) {
        ((ParsedRange) aggregation)
                .getBuckets()
                .listIterator()
                .forEachRemaining(e -> {
                    Object key = e.getKey();
                    Object from = e.getFrom();
                    Object to = e.getTo();
                    long docCount = e.getDocCount();

                    System.err.println("============ " + key + "============");
                    System.err.println("from = " + from);
                    System.err.println("to = " + to);
                    System.err.println("docCount = " + docCount);
                });
        ;

    }

    private void printParsedGeoCentroid(Aggregation aggregation) {
        GeoPoint centroid = ((ParsedGeoCentroid) aggregation).centroid();
        System.err.println("centroid = " + centroid);
    }

    private void printParsedGeoBounds(Aggregation aggregation) {
        ParsedGeoBounds parsedGeoBounds = (ParsedGeoBounds) aggregation;
        GeoPoint topLeft = parsedGeoBounds.topLeft();
        GeoPoint bottomRight = parsedGeoBounds.bottomRight();
        System.err.println("topLeft = " + topLeft);
        System.err.println("bottomRight = " + bottomRight);
    }

    private void printParsedTDigestPercentileRanks(Aggregation aggregation) {
        ((ParsedTDigestPercentileRanks) aggregation)
                .forEach(e -> System.err.println(e.getValue() + " : " + e.getPercent()));
    }

    private void printParsedTDigestPercentiles(Aggregation aggregation) {
        ((ParsedTDigestPercentiles) aggregation).forEach(e -> System.err.println(e.getPercent() + " : " + e.getValue()));
    }

    private void printParsedCardinality(Aggregation aggregation) {
        String name = aggregation.getName();
        long value = ((ParsedCardinality) aggregation).getValue();

        System.err.println(name + " : " + value);
    }

    private void printParsedStringTerms(Aggregation aggregation) {
        ParsedStringTerms parsedStringTerms = (ParsedStringTerms) aggregation;
        Map<Object, Long> map = parsedStringTermsToMap(parsedStringTerms);
        long sumOfOtherDocCounts = parsedStringTerms.getSumOfOtherDocCounts();
        long docCountError = parsedStringTerms.getDocCountError();
        System.err.println("sumOfOtherDocCounts = " + sumOfOtherDocCounts);
        System.err.println("docCountError = " + docCountError);
        map.entrySet()
                .forEach(System.err::println);
    }

    private Map<Object, Long> parsedStringTermsToMap(ParsedStringTerms aggregation) {
        return aggregation
                .getBuckets()
                .stream()
                .collect(toMap(MultiBucketsAggregation.Bucket::getKey, MultiBucketsAggregation.Bucket::getDocCount));
    }

    private void printParsedExtendedStats(Aggregation aggregation) {
        ParsedExtendedStats stats = (ParsedExtendedStats) aggregation;
        long count = stats.getCount();
        double min = stats.getMin();
        double max = stats.getMax();
        double sum = stats.getSum();
        double avg = stats.getAvg();
        double sumOfSquares = stats.getSumOfSquares();
        double variance = stats.getVariance();
        double stdDeviation = stats.getStdDeviation();
        double stdDeviationBoundUpper = stats.getStdDeviationBound(ExtendedStats.Bounds.UPPER);
        double stdDeviationBoundLower = stats.getStdDeviationBound(ExtendedStats.Bounds.LOWER);
        String name = stats.getName();

        System.err.println("======" + name + "======");
        System.err.println("count = " + count);
        System.err.println("min = " + min);
        System.err.println("max = " + max);
        System.err.println("sum = " + sum);
        System.err.println("avg = " + avg);
        System.err.println("sumOfSquares = " + sumOfSquares);
        System.err.println("variance = " + variance);
        System.err.println("stdDe = " + stdDeviation);
        System.err.println("stdDeviationBoundUpper = " + stdDeviationBoundUpper);
        System.err.println("stdDeviationBoundLower = " + stdDeviationBoundLower);
    }

    private void printSingleValue(Aggregation aggregation) {
        String name = aggregation.getName();
        double value = ((NumericMetricsAggregation.SingleValue) aggregation).value();

        System.err.println(name + " : " + value);
    }

    private void printParsedStats(Aggregation aggregation) {
        ParsedStats stats = (ParsedStats) aggregation;
        long count = stats.getCount();
        double min = stats.getMin();
        double max = stats.getMax();
        double sum = stats.getSum();
        double avg = stats.getAvg();
        String name = stats.getName();
        System.err.println("======" + name + "======");
        System.err.println("count = " + count);
        System.err.println("min = " + min);
        System.err.println("max = " + max);
        System.err.println("sum = " + sum);
        System.err.println("avg = " + avg);
    }


}
