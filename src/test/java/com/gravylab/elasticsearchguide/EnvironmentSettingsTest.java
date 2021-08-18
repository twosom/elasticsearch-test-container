package com.gravylab.elasticsearchguide;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthRequest;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.testcontainers.elasticsearch.ElasticsearchContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

@Testcontainers
public class EnvironmentSettingsTest {


    @Container
    static ElasticsearchContainer container =
            new ElasticsearchContainer(DockerImageName.parse("docker.elastic.co/elasticsearch/elasticsearch:7.12.0"))
                    .withEnv("cluster.name", "javacafe-cluster")
                    .withEnv("node.name", "javacafe-node1")
                    .withEnv("network.host", "0.0.0.0")
            ;


    RestHighLevelClient client;


    @BeforeEach
    void beforeEach() {
        this.client = getRestClient();
    }

    @AfterEach
    void afterEach() throws IOException {
        client.close();
    }


    private RestHighLevelClient getRestClient() {
        BasicCredentialsProvider credentialProvider = new BasicCredentialsProvider();
        credentialProvider.setCredentials(AuthScope.ANY,
                new UsernamePasswordCredentials("elasticsearch", "elasticsearch"));
        RestClientBuilder builder = RestClient.builder(HttpHost.create(container.getHttpHostAddress()))
                .setHttpClientConfigCallback(
                        httpClientBuilder -> httpClientBuilder.setDefaultCredentialsProvider(credentialProvider)
                );
        return new RestHighLevelClient(builder);
    }


    @DisplayName("가상 테스트 컨테이너에서 정상 작동하는지 확인하는 테스트")
    @Test
    void test_rest_client() throws Exception {
        ClusterHealthRequest clusterHealthRequest = new ClusterHealthRequest();
        ClusterHealthResponse health = client.cluster().health(clusterHealthRequest, RequestOptions.DEFAULT);
        System.out.println("health = " + health);
    }

    @DisplayName("테스트 인덱스 생성")
    @Test
    void create_test_index() throws Exception {
        Map<String, Object> settings = new HashMap<>();
        settings.put("number_of_shards", 3);
        settings.put("number_of_replicas", 0);
        CreateIndexRequest createIndexRequest = new CreateIndexRequest("starwars")
                .settings(settings);


        CreateIndexResponse createIndexResponse = client.indices().create(createIndexRequest, RequestOptions.DEFAULT);
        System.out.println("createIndexResponse = " + createIndexResponse);
    }

}
