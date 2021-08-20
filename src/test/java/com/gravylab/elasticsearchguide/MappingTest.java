package com.gravylab.elasticsearchguide;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

public class MappingTest extends CommonTestClass {


    public static final String NUMBER_OF_SHARDS = "number_of_shards";
    public static final String NUMBER_OF_REPLICAS = "number_of_replicas";

    @DisplayName("매핑 실패 테스트")
    @Test
    void mapping_fail_test() throws IOException {
        removeIndexIfExists("movie");
        CreateIndexRequest createIndexRequest = new CreateIndexRequest("movie");
        CreateIndexResponse createIndexResponse = client.indices().create(createIndexRequest, RequestOptions.DEFAULT);

        IndexRequest request1 = new IndexRequest("movie");
        Map<String, Object> source = new HashMap<>();
        source.put("movieCd", 20173732);
        source.put("movieNm", "캡틴 아메리카");
        request1.source(source);

        client.index(request1, RequestOptions.DEFAULT);


        IndexRequest request2 = new IndexRequest("movie");
        source = new HashMap<>();
        source.put("movieCd", "XT001");
        source.put("movieNm", "아이언맨");

        // 실패를 해야 성공한다.
        assertThrows(ActionRequestValidationException.class, () -> {
            client.index(request2, RequestOptions.DEFAULT);
        });
    }



    @DisplayName("인덱스 생성")
    void create_index_test() throws IOException {
        removeIndexIfExists("movie");

        CreateIndexRequest createIndexRequest = new CreateIndexRequest("movie");
        XContentBuilder settingBuilder = createSettingBuilder();
        XContentBuilder mappingBuilder = createMappingBuilder();
    }

    private XContentBuilder createMappingBuilder() throws IOException {
        XContentBuilder mappingBuilder = XContentFactory.jsonBuilder();
        mappingBuilder.startObject();
        {
            mappingBuilder.startObject("properties");
            {
                defineField(mappingBuilder, "movieCd", "keyword");
                defineFieldWithAnalyzer(mappingBuilder, "movieNm", "text");
                defineFieldWithAnalyzer(mappingBuilder, "movieNmEn", "text");
                //TODO 필드 더 정의 해야됨.
            }
        }
        return mappingBuilder;
    }

    private void defineField(XContentBuilder mappingBuilder, String field, String type) throws IOException {
        mappingBuilder.startObject(field);
        {
            mappingBuilder.field("type", type);
        }
        mappingBuilder.endObject();
    }
    private void defineFieldWithAnalyzer(XContentBuilder mappingBuilder, String field, String type) throws IOException {
        mappingBuilder.startObject(field);
        {
            mappingBuilder.field("type", type);
            mappingBuilder.field("analyzer", "standard");
        }
        mappingBuilder.endObject();
    }

    private XContentBuilder createSettingBuilder() throws IOException {
        XContentBuilder settingBuilder = XContentFactory.jsonBuilder();
        settingBuilder.startObject();
        {
            settingBuilder.field(NUMBER_OF_SHARDS, 5);
            settingBuilder.field(NUMBER_OF_REPLICAS, 1);
        }
        settingBuilder.endObject();
        return settingBuilder;
    }
}
