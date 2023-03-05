package com.producer.producer;

import jakarta.inject.Singleton;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

import static com.producer.ProjectConstants.BOOTSTRAP_SERVER_VALUE;

@Singleton
public class KafkaDispatcher {
    private static final Logger log = LoggerFactory.getLogger(KafkaDispatcher.class);

    public static void main(String[] args) {
        log.info("Producer KAFKA working here");

        Properties properties = new Properties();

        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER_VALUE);


        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        ProducerRecord<String, String> record = new ProducerRecord<>("TOPICO_TESTE",
                "First message");

        producer.send(record, new Callback() {
            @Override
            public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                if(e == null){
                    log.info("Received new metadata. \n" +
                            "Topic:" + recordMetadata.topic() + "\n" +
                            "Partition: " + recordMetadata.partition() + "\n" +
                            "Offset: " + recordMetadata.offset() + "\n" +
                            "Timestamp: " + recordMetadata.timestamp());
                }else {
                    log.error("Error caught trying send message: {}", e.getMessage());
                }
            }
        });
        producer.flush();
        producer.close();

    }
}
