package com.kong.assignment;

import org.apache.http.HttpHost;
import org.opensearch.client.RestClient;
import org.opensearch.client.RestHighLevelClient;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import static com.kong.assignment.Constants.*;

@Configuration
public class OpenSearchConfig {

    @Bean
    public RestHighLevelClient client() {
        // Create RestHighLevelClient
        return new RestHighLevelClient(RestClient.builder(new HttpHost(LOCALHOST, OPEN_SEARCH_PORT, HTTP_PROTOCOL)));
    }
}
