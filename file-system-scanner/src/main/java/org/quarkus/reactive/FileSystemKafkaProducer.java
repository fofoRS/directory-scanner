package org.quarkus.reactive;


import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.quarkus.vertx.ConsumeEvent;
import io.vertx.core.json.JsonObject;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.PostConstruct;
import javax.enterprise.context.ApplicationScoped;
import java.util.Properties;

@ApplicationScoped
public class FileSystemKafkaProducer {
    Logger logger = LoggerFactory.getLogger(FileSystemKafkaProducer.class);

    static final String SYSTEM_CHANGE_TOPIC = "file.system.watcher.topic";

    Properties producerConfig;
    KafkaProducer<String,String> kafkaProducer;

    @PostConstruct
    public void init() {
        producerConfig = new Properties();
        producerConfig.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        producerConfig.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerConfig.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());


        // idempotence consumer (safe consumer)
        producerConfig.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        producerConfig.setProperty(ProducerConfig.ACKS_CONFIG, "all");
        producerConfig.setProperty(ProducerConfig.RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE));
        producerConfig.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5"); // IF kafka >= 1.1

        producerConfig.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
        producerConfig.setProperty(ProducerConfig.LINGER_MS_CONFIG, Integer.toString(20));
        kafkaProducer = new KafkaProducer<String, String>(producerConfig);

    }


    @ConsumeEvent(value ="file.system.events")
    public void publishFileSystemChange(String fileName) {
        logger.info("Preparing Message to send to kafka -- {}", fileName);
        ProducerRecord<String,String> record = null;
            record = new ProducerRecord<>(
                    SYSTEM_CHANGE_TOPIC,
                   fileName);
        kafkaProducer.send(record,(data,exception) -> {
            logger.info("Message Sent to offset {}, topic {}", data.offset(),data.topic());
        });
    }
}
