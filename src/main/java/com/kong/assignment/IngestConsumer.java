package com.kong.assignment;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.common.util.StringUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.action.update.UpdateRequest;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.common.xcontent.XContentType;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Map;
import java.util.Properties;

import static com.kong.assignment.Constants.*;

@Slf4j
@Component
public class IngestConsumer {
    private final RestHighLevelClient restHighLevelClient;
    private final ObjectMapper objectMapper;

    @Autowired
    public IngestConsumer(RestHighLevelClient restHighLevelClient, ObjectMapper objectMapper) {
        this.restHighLevelClient = restHighLevelClient;
        this.objectMapper = objectMapper;
    }


    private Properties getKafkaConsumerProperties() {
        Properties properties = new Properties();

        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, CONSUMER_GROUP);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

        return properties;
    }


    public void writeToOpenSearch() {

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(getKafkaConsumerProperties());

        final Thread mainThread = Thread.currentThread();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                log.info("Shutting down consumer");
                consumer.wakeup();
                mainThread.join();
            } catch (InterruptedException e) {
                log.error("Error shutting down consumer: {}", e.getMessage());
            }
        }));


        consumer.subscribe(Arrays.asList(CDC_EVENTS_TOPIC));

        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(3000));
                int received = records.count();

                BulkRequest bulkRequest = preprocessBatch(records);
                int validEvent = bulkRequest.numberOfActions();
                int attempt = 0;

                while (validEvent > 0 && attempt < MAX_RETRIES) {
                    try {
                        BulkResponse bulkResponse = restHighLevelClient.bulk(bulkRequest, RequestOptions.DEFAULT);
                        consumer.commitSync();
                        log.trace("OpenSearch bulkRequest Count: {} bulkResponse Count: {}", validEvent, bulkResponse.getItems().length);
                        break;
                    } catch (IOException e) {
                        if (attempt == 0) {
                            log.error("Error indexing document: {}", e.getMessage());
                        }
                        attempt++;
                    }
                }

                if (attempt == MAX_RETRIES) {
                    //We can send these message to a dead letter queue and continue processing by committing the offset
                    log.error("Max retries reached for batch: {}", validEvent);
                    throw new RuntimeException("Max retries reached for batch");
                }

                //We can emit time series metrics here for monitoring
                log.info("Batch received: {}, event processed: {}", received, validEvent);
            }
        } catch (WakeupException e) {
            log.info("Received shutdown signal!");
        } catch (Exception e) {
            log.error("Error processing events: {}", e.getMessage());
        } finally {
            consumer.close();
            try {
                restHighLevelClient.close();
            } catch (IOException e) {
                log.error("Error closing OpenSearch client: {}", e.getMessage());
            }
            log.info("Consumer gracefully shutdown");
        }
    }

    private BulkRequest preprocessBatch(ConsumerRecords<String, String> records) {

        BulkRequest bulkRequest = new BulkRequest();

        for (ConsumerRecord<String, String> record : records) {
            String line = record.value();

            EventData event = null;

            try {
                event = objectMapper.readValue(line, EventData.class);
            } catch (JsonProcessingException e) {
                log.error("Error parsing JSON: [{}] - Exception: ", line, e);
            }

            if (event != null && event.getAfter() != null && event.getAfter().getKey() != null) {
                String key = event.getAfter().getKey();
                if (StringUtils.isNotEmpty(key)) {
                    String[] key_parts = key.split("/");
                    if (key_parts.length == 5 && KONG_ENTITIES.contains(key_parts[3])) {
                        String userId = key_parts[1];
                        String serviceId = key_parts[3];
                        Map<String, Object> eventMap = event.getAfter().getValue().getObject();
                        String documentId = (String) eventMap.remove(DOCUMENT_ID);
                        eventMap.put(USER_ID, userId);
                        eventMap.put(ENTITY_TYPE, serviceId);

                        bulkRequest.add(new UpdateRequest(OPEN_SEARCH_CDC_INDEX, documentId).doc(eventMap, XContentType.JSON).upsert(eventMap, XContentType.JSON));
                    }
                } else {
                    log.error("Invalid key: {}", line);
                }
            } else {
                log.error("Invalid event: {}", line);
            }
        }

        return bulkRequest;
    }
}
