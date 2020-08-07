package org.quarkus.reactive;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.*;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static java.nio.file.StandardWatchEventKinds.*;

public class DirectoryEventProducer {

    Logger logger = LoggerFactory.getLogger(DirectoryEventProducer.class);

    static final String SYSTEM_CHANGE_TOPIC = "file.system.watcher.topic";
    static final Map<WatchKey, Path> keys = new HashMap<>();

    Properties producerConfig;
    KafkaProducer<String, String> kafkaProducer;
    WatchService watcher;

    public DirectoryEventProducer() throws IOException {
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

    public void watch(String path) {
        new Thread(() -> {
            logger.info("Watching dir...");
            try {
                watcher = FileSystems.getDefault().newWatchService();
            } catch (IOException ioException) {
                ioException.printStackTrace();
            }
            registerPath(Paths.get(path));
            for (; ;) {
                logger.info("Iteration through key events");
                WatchKey key = null;
                try {
                    key = watcher.poll(2, TimeUnit.SECONDS);
                } catch (InterruptedException ex) {
                    ex.printStackTrace();
                }
                Path currentDir = keys.get(key);
                if (currentDir == null || key == null) {
                    continue;
                }
                for (WatchEvent<?> event : key.pollEvents()) {
                    logger.info("Polling events for watchedKey");
                    WatchEvent<Path> watchEvent = (WatchEvent<Path>) event;
                    WatchEvent.Kind<Path> kind = watchEvent.kind();
                    switch (kind.name()) {
                        case "OVERFLOW":
                            continue;
                        default:
                            processFileSystemEvent(currentDir, watchEvent, kind);
                    }
                    key.reset();
                }
            }
        }).start();

    }

    private void processFileSystemEvent(Path currentDir, WatchEvent<Path> watchedEvent,
                                        WatchEvent.Kind<Path> eventKind) {
        Path filePath = watchedEvent.context();
        Path child = currentDir.resolve(filePath);
        if (watchedEvent.count() > 1) {
            logger.info("Repeated Event --- skip archive processing");
            return;
        }
        System.out.format("Logging Event data  %s: %s\n", eventKind.name(), child);
        publishFileSystemChange(child.toFile().getName());
        registerPath(child);
    }

    private void registerPath(Path path) {
        WatchKey key = null;
        try {
            logger.info("Registering new Archive {}", path.toFile().getName());
            if (Files.isDirectory(path)) {
                key = path.register(watcher, OVERFLOW, ENTRY_CREATE, ENTRY_DELETE, ENTRY_MODIFY);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        keys.put(key, path);
    }

    private void publishFileSystemChange(final String fileName) {
        logger.info("Preparing Message to send to kafka -- {}", fileName);
        ProducerRecord<String, String> record = null;
        record = new ProducerRecord<String, String>(
                SYSTEM_CHANGE_TOPIC, fileName);
        kafkaProducer.send(record, (data, exception) -> {
            logger.info("Message Sent to offset {},topic {}", data.offset(), data.topic());
        });
    }
}
