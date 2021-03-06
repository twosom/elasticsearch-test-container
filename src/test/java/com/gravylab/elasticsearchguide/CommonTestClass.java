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
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.testcontainers.containers.BindMode;
import org.testcontainers.elasticsearch.ElasticsearchContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.*;

@Testcontainers
public class CommonTestClass {


    public static final String ELASTICSEARCH_ANALYSIS_DIRECTORY = "/usr/share/elasticsearch/config/analysis";
    public static final String NUMBER_OF_SHARDS = "number_of_shards";
    public static final String NUMBER_OF_REPLICAS = "number_of_replicas";
    @Container
    static ElasticsearchContainer container =
            new ElasticsearchContainer(DockerImageName.parse("docker.elastic.co/elasticsearch/elasticsearch:7.12.0"))
                    .withEnv("cluster.name", "javacafe-cluster")
                    .withEnv("node.name", "javacafe-node1")
                    .withEnv("network.host", "0.0.0.0")
                    .withEnv("path.repo", "/usr/share/elasticsearch/backup")
                    .withClasspathResourceMapping("synonym.txt", "/usr/share/elasticsearch/config/synonym.txt", BindMode.READ_ONLY)
                    .withClasspathResourceMapping("search_example", "/usr/share/elasticsearch/backup/search", BindMode.READ_WRITE)
                    .withClasspathResourceMapping("agg_example", "/usr/share/elasticsearch/backup/agg", BindMode.READ_WRITE)
            ;
    ;


    RestHighLevelClient testContainerClient;
    RestHighLevelClient dockerClient;

    @BeforeAll
    static void beforeAll() throws IOException, InterruptedException {
        ElasticsearchContainer container = CommonTestClass.container;
        assertNotNull(container);
        //TODO ??????????????? resources ??????????????? ?????? synonym.txt ??? ????????? ??????????????? ????????? ??????
        container.execInContainer("mkdir", ELASTICSEARCH_ANALYSIS_DIRECTORY);
        container.execInContainer("mkdir", "/usr/share/elasticsearch/backup/movie_data_backup");
        container.execInContainer("mv", "/usr/share/elasticsearch/config/synonym.txt", ELASTICSEARCH_ANALYSIS_DIRECTORY);
    }

    @BeforeEach
    void beforeEach() {
        this.testContainerClient = getTestContainerRestClient();
        this.dockerClient = getDockerRestClient();



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
        testContainerClient.close();
        dockerClient.close();
    }


    private RestHighLevelClient getTestContainerRestClient() {


        BasicCredentialsProvider credentialProvider = new BasicCredentialsProvider();
        credentialProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials("elasticsearch", "elasticsearch"));

        RestClientBuilder builder = RestClient.builder(HttpHost.create(container.getHttpHostAddress()));

        return new RestHighLevelClient(
                builder
                        .setHttpClientConfigCallback(
                                httpClientBuilder ->
                                        httpClientBuilder
                                                .setDefaultCredentialsProvider(credentialProvider)
                                                .setConnectionReuseStrategy((response, context) -> true)
                                                .setKeepAliveStrategy((response, context) -> 9999999999L)

                        )
        );


    }

    protected void removeIndexIfExists(String index, RestHighLevelClient client) throws IOException {
        if (isExistsIndex(index, client)) {
            DeleteIndexRequest deleteIndexRequest = new DeleteIndexRequest(index);
            AcknowledgedResponse deleteResponse = client.indices().delete(deleteIndexRequest, RequestOptions.DEFAULT);
            Assertions.assertTrue(deleteResponse.isAcknowledged());
            System.out.println("[" + index + "] ????????? ????????? ?????????????????????.");
        }
    }

    protected boolean isExistsIndex(String index, RestHighLevelClient client) throws IOException {
        GetIndexRequest getIndexRequest = new GetIndexRequest(index);
        return client.indices().exists(getIndexRequest, RequestOptions.DEFAULT);
    }

    /**
     * {@link RestHighLevelClient} ??? ElasticSearch ??? ????????? ?????? ??? ??????????????? ??????????????? ???????????? ???????????????.
     *
     * @param createIndexResponse ????????? ?????? ??? ???????????? {@link CreateIndexResponse} ????????? ????????????.
     * @param indexName           ????????? ????????? ????????? ????????????.
     */
    protected void assertCreatedIndex(CreateIndexResponse createIndexResponse, String indexName) {
        assertTrue(createIndexResponse.isAcknowledged());
        assertTrue(createIndexResponse.isShardsAcknowledged());
        assertEquals(createIndexResponse.index(), indexName);
    }
}
