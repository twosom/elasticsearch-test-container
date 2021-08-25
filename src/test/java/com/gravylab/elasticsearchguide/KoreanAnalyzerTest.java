package com.gravylab.elasticsearchguide;

import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.indices.*;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 사전에 Docker 를 실행 후 각각의 인스턴스에 analysis-nori 플러그인을 설치해준다.
 */
public class KoreanAnalyzerTest extends CommonTestClass {


    public static final String INDEX_NAME = "nori_analyzer";
    public static final String TOKENIZER_NAME = "nori_user_dict_tokenizer";
    public static final String DECOMPOUND_MODE = "decompound_mode";
    public static final String ANALYZER_NAME = "nori_token_analyzer";
    public static final String NORI_PART_OF_SPEECH = "nori_part_of_speech";

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

    @DisplayName("앞서 생성한 인덱스에 토큰 필터 정보 추가하기")
    @Test
    void add_token_filter() throws Exception {
        index_with_user_dictionary();
        CloseIndexRequest closeIndexRequest = new CloseIndexRequest(INDEX_NAME);
        CloseIndexResponse closeIndexResponse = dockerClient.indices().close(closeIndexRequest, RequestOptions.DEFAULT);
        assertTrue(closeIndexResponse.isAcknowledged());

        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();
        {
            builder.startObject("analysis");
            {

                builder.startObject("analyzer");
                {
                    builder.startObject("nori_stoptags_analyzer");
                    {
                        builder.field("tokenizer", "nori_tokenizer");
                        builder.startArray("filter");
                        {
                            builder.value("nori_posfilter");
                        }
                        builder.endArray();
                    }
                    builder.endObject();
                }
                builder.endObject();


                builder.startObject("filter");
                {
                    builder.startObject("nori_posfilter");
                    {
                        builder.field("type", NORI_PART_OF_SPEECH);
                        builder.startArray("stoptags");
                        {
                            builder.value("E")
                                    .value("IC")
                                    .value("J")
                                    .value("MAG")
                                    .value("MAJ")
                                    .value("MAJ")
                                    .value("MM")
                                    .value("NA")
                                    .value("NR")
                                    .value("SC")
                                    .value("SE")
                                    .value("SF")
                                    .value("SH")
                                    .value("SL")
                                    .value("SN")
                                    .value("SP")
                                    .value("SSC")
                                    .value("SSO")
                                    .value("SY")
                                    .value("UNA")
                                    .value("UNKNOWN")
                                    .value("VA")
                                    .value("VCN")
                                    .value("VCP")
                                    .value("VSV")
                                    .value("VV")
                                    .value("VX")
                                    .value("XPN")
                                    .value("XR")
                                    .value("XSA")
                                    .value("XSN")
                                    .value("XSV");
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


        PutMappingRequest putMappingRequest = new PutMappingRequest(INDEX_NAME)
                .source(builder);


        AcknowledgedResponse putMappingResponse = dockerClient.indices().putMapping(putMappingRequest, RequestOptions.DEFAULT);
        assertTrue(putMappingResponse.isAcknowledged());

    }


}
