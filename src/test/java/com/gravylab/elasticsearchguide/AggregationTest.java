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
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.search.aggregations.bucket.histogram.ParsedDateHistogram;
import org.elasticsearch.search.aggregations.bucket.histogram.ParsedHistogram;
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
import static org.elasticsearch.search.aggregations.PipelineAggregatorBuilders.derivative;
import static org.elasticsearch.search.aggregations.PipelineAggregatorBuilders.maxBucket;
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

    //TODO 히스토그램 집계(Histogram Aggregations) 는 숫자 범위를 처리하기 위한 집계다.
    // 지정한 범위 내에서 집계를 수행하는 범위 집계와는 달리 지정한 수치가 간격을 나타내고, 이 간격의 범위 내에서 집계를 수행한다.
    @DisplayName("히스토그램 집계")
    @Test
    void aggregation_with_histogram() throws Exception {
        restore_snapshot();
        SearchRequest searchRequest = new SearchRequest(APACHE_WEB_LOG)
                .source(
                        new SearchSourceBuilder()
                                .size(0)
                                .aggregation(
                                        histogram("bytes_histogram")
                                                .field("bytes")
                                                .interval(10000)
                                )
                );
        printAggregation(searchRequest, "bytes_histogram");
    }

    //TODO 위의 결과를 보면 문서 개수가 0인 간격도 포함되 있다.
    // 만약 문서가 존재하지 않는 구간은 필요하지 않다면 아래와 같이 촤소 문서 수(min_doc_count) 를 설정해서 해당 구간을 제외시킬 수 있다.
    @DisplayName("히스토그램에 최소 문서 수 설정")
    @Test
    void aggregation_with_histogram_and_min_doc_count() throws Exception {
        restore_snapshot();
        SearchRequest searchRequest = new SearchRequest(APACHE_WEB_LOG)
                .source(
                        new SearchSourceBuilder()
                                .size(0)
                                .aggregation(
                                        histogram("bytes_histogram")
                                                .field("bytes")
                                                .interval(10000)
                                                .minDocCount(1)
                                )
                );
        printAggregation(searchRequest, "bytes_histogram");
    }


    //TODO 날짜 히스토그램 집계(Date Histogram Aggregation) 는 다중 버킷 집계에 속하며 히스토글매 집계와 유사하다.
    // 히스토그램 집계는 숫자 값을 간격으로 삼아 집계를 수행한 반면 날짜 히스토그램 집계는 분, 시간, 월, 연도를 구간으로 집계를 수행할 수 있다.
    // 분 단위로 얼마만큼의 사용자 유입이 있었는지 확인해보기 위해 다음과 같이 집계를 수행해 보았다.
    @DisplayName("날짜 히스토그램 집계")
    @Test
    void aggregation_with_date_histogram() throws Exception {
        restore_snapshot();
        SearchRequest searchRequest = new SearchRequest(APACHE_WEB_LOG)
                .source(
                        new SearchSourceBuilder()
                                .size(0)
                                .aggregation(
                                        dateHistogram("daily_request_count")
                                                .field("timestamp")
                                                .calendarInterval(DateHistogramInterval.MINUTE)
                                )
                );
        printAggregation(searchRequest, "daily_request_count");
    }

    @DisplayName("날짜 히스토그램 날짜 방식 변경")
    @Test
    void aggregation_with_date_histogram_change_date_type() throws Exception {
        restore_snapshot();
        SearchRequest searchRequest = new SearchRequest(APACHE_WEB_LOG)
                .source(
                        new SearchSourceBuilder()
                                .size(0)
                                .aggregation(
                                        dateHistogram("daily_request_count")
                                                .field("timestamp")
                                                .calendarInterval(DateHistogramInterval.DAY)
                                                .format("yyyy-MM-dd")
                                )
                );

        printAggregation(searchRequest, "daily_request_count");
    }


    //TODO 텀즈 집계(Terms Aggregation) 는 버킷이 동적으로 생성되는 다중 버킷 집계다.
    // 집계 시 지정한 필드에 대해 빈도수가 높은 텀의 순위로 결과가 반환된다.
    // 이를 통해 가장 많이 접속하는 사용자를 알아낸다거나 국가별로 어느 정도의 빈도로 서버에 접속하는지 등의 집계를 수행할 수 있다.
    // 텀즈 집계를 통해 아파치 서버로 얼마만큼의 요청이 들어왔는지를 국가별로 집계해보자.
    @DisplayName("텀즈 집계")
    @Test
    void aggregation_with_term() throws Exception {
        restore_snapshot();
        SearchRequest searchRequest = new SearchRequest(APACHE_WEB_LOG)
                .source(
                        new SearchSourceBuilder()
                                .size(0)
                                .aggregation(
                                        terms("request count by country")
                                                .field("geoip.country_name.keyword")
                                                .size(100)
                                )
                );

        printAggregation(searchRequest, "request count by country");
    }

    @DisplayName("파이프라인 집계")
    @Test
    void aggregation_with_pipeline() throws Exception {
        restore_snapshot();
        SearchRequest searchRequest = new SearchRequest(APACHE_WEB_LOG)
                .source(
                        new SearchSourceBuilder()
                                .size(0)
                                .aggregation(
                                        dateHistogram("histo")
                                                .field("timestamp")
                                                .calendarInterval(DateHistogramInterval.MINUTE)
                                                .subAggregation(
                                                        sum("bytes_sum")
                                                                .field("bytes")
                                                )
                                )
                                .aggregation(
                                        maxBucket("max_bytes", "histo>bytes_sum")
                                ));

        SearchResponse searchResponse = testContainerClient.search(searchRequest, RequestOptions.DEFAULT);
        System.err.println("searchResponse = " + searchResponse);
    }

    @DisplayName("파생 집계")
    @Test
    void aggregation_with_derivative() throws Exception {
        restore_snapshot();
        SearchRequest searchRequest = new SearchRequest(APACHE_WEB_LOG)
                .source(
                        new SearchSourceBuilder()
                                .size(0)
                                .aggregation(
                                        dateHistogram("histo")
                                                .field("timestamp")
                                                .calendarInterval(DateHistogramInterval.DAY)
                                                .subAggregation(
                                                        sum("bytes_sum")
                                                                .field("bytes")
                                                )
                                                .subAggregation(
                                                        derivative("sum_deriv", "bytes_sum")
                                                )
                                )
                );
        SearchResponse searchResponse = testContainerClient.search(searchRequest, RequestOptions.DEFAULT);
        System.out.println("searchResponse = " + searchResponse);
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
            case "ParsedHistogram":
                printParsedHistogram(aggregation);
                break;
            case "ParsedDateHistogram":
                printParsedDateHistogram(aggregation);
                break;
            default:
                printSingleValue(aggregation);
                break;
        }


    }

    private void printParsedDateHistogram(Aggregation aggregation) {
        ((ParsedDateHistogram) aggregation)
                .getBuckets()
                .forEach(e -> System.err.println(e.getKeyAsString() + " : " + e.getDocCount()));

    }

    private void printParsedHistogram(Aggregation aggregation) {
        ((ParsedHistogram) aggregation)
                .getBuckets()
                .forEach(e -> System.err.println(e.getKey() + " : " + e.getDocCount()));
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
        ((ParsedStringTerms) aggregation)
                .getBuckets()
                .forEach(e -> System.err.println(e.getKey() + " : " + e.getDocCount()));
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
