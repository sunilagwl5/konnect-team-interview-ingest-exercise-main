package com.kong.assignment;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.stereotype.Component;

import java.io.*;
import java.util.Properties;

import static com.kong.assignment.Constants.*;

@Slf4j
@Component
public class IngestProducer {

    private Properties getKafkaProducerProperties() {
        Properties properties = new Properties();

        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS_CONFIG);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "5");
        properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");

        return properties;
    }

    public void readJsonFile() {


        KafkaProducer<String, String> producer = new KafkaProducer<>(getKafkaProducerProperties());

        // if it is a long running process, we can add a shutdown hook to close the producer
//        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
//            log.info("Shutting down producer");
//            producer.close();
//        }));

        try (InputStream inputStream = new FileInputStream(FILE_PATH);
             BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream))) {

            String line;

            while ((line = reader.readLine()) != null) {

                ProducerRecord<String, String> record = new ProducerRecord<>(CDC_EVENTS_TOPIC, null, line);
                producer.send(record, new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                        if (e != null) {
                            //We can send the message to a dead letter queue
                            log.error("Error sending message {} to kafka: {}", recordMetadata.toString(), e.getMessage());
                        }
                    }
                });
            }
        } catch (IOException e) {
            log.error("Error reading file: {}", e.getMessage());
        } finally {
            producer.close();
        }
    }
}
