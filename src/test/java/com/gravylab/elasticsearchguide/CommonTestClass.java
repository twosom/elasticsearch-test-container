package com.gravylab.elasticsearchguide;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.testcontainers.elasticsearch.ElasticsearchContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.io.IOException;

@Testcontainers
public class CommonTestClass {

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

    protected void removeIndexIfExists(String index) throws IOException {
        if (isExistsIndex(index)) {
            DeleteIndexRequest deleteIndexRequest = new DeleteIndexRequest(index);
            AcknowledgedResponse deleteResponse = client.indices().delete(deleteIndexRequest, RequestOptions.DEFAULT);
            Assertions.assertTrue(deleteResponse.isAcknowledged());
            System.out.println("[" + index + "] 인덱스 삭제가 완료되었습니다.");
        }
    }

    protected boolean isExistsIndex(String index) throws IOException {
        GetIndexRequest getIndexRequest = new GetIndexRequest(index);
        return client.indices().exists(getIndexRequest, RequestOptions.DEFAULT);
    }
}
