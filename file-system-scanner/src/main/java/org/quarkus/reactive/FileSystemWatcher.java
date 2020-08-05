package org.quarkus.reactive;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.quarkus.runtime.Startup;
import io.vertx.core.json.JsonObject;
import io.vertx.mutiny.core.eventbus.EventBus;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.PostConstruct;
import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import java.io.IOException;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static java.nio.file.StandardWatchEventKinds.*;

@ApplicationScoped
@Startup
public class FileSystemWatcher {

    Logger logger = LoggerFactory.getLogger(FileSystemWatcher.class);

    static final Map<WatchKey, Path> keys = new HashMap<>();
    static final String directory = "/Users/rodolfo/Documents/personal/training/learning/quarkus/quarkus-file-system-scanner/file-system-scanner/testDir";
    static final String FILE_SYSTEM_EVENT_ADDRESS = "file.system.events";
    static final String SYSTEM_CHANGE_TOPIC = "file.system.watcher.topic";

    WatchService watcher;
    Properties producerConfig;
    KafkaProducer<String,String> kafkaProducer;

    @Inject
    EventBus eventBus;

    @PostConstruct
    public void init() throws IOException {
        producerConfig = new Properties();
        producerConfig.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        producerConfig.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerConfig.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());


        // idempotence consumer (safe consumer)
        producerConfig.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        producerConfig.setProperty(ProducerConfig.ACKS_CONFIG, "all");
        producerConfig.setProperty(ProducerConfig.RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE));
        producerConfig.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5"); // IF kafka >= 1.1

//        producerConfig.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG,"snappy");
//        producerConfig.setProperty(ProducerConfig.LINGER_MS_CONFIG, Integer.toString(20));
        kafkaProducer = new KafkaProducer<String, String>(producerConfig);

        watcher = FileSystems.getDefault().newWatchService();
        initPathRegistration(Paths.get(directory));
        watch();
    }

    public void watch() {
        logger.info("Watching dir...");
        for (;;) {
            logger.info("Iteration through key events");
            WatchKey key = null;
            try {
                key = watcher.take();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            Path currentDir = keys.get(key);
            if (currentDir == null) {
                logger.error("Not recognized watched key");
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
    }

    private void processFileSystemEvent(Path currentDir,WatchEvent<Path> watchedEvent,  WatchEvent.Kind<Path> eventKind) {
        Path filePath = watchedEvent.context();
        Path child = currentDir.resolve(filePath);
        if(watchedEvent.count() > 0) {
            logger.info("Repeated Event --- skip archive processing");
            return;
        }
        System.out.format("Logging Event data  %s: %s\n", eventKind.name(), child);
        publishFileSystemEvent(child);
        registerPath(child);
    }

    private void initPathRegistration(Path path) {
        try {
            if (Files.isDirectory(path, LinkOption.NOFOLLOW_LINKS)) {
                Files.walkFileTree(path, new SimpleFileVisitor<Path>() {
                    public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attr) {
                        if(path.compareTo(dir) != 0) {
                            registerPath(dir);
                        }
                        return FileVisitResult.CONTINUE;
                    }
                });
            } else {
                registerPath(path);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void registerPath(Path path) {
        WatchKey key = null;
        try {
            logger.info("Registering new Archive {}", path.toFile().getName());
            key = path.register(watcher, OVERFLOW, ENTRY_CREATE, ENTRY_DELETE, ENTRY_MODIFY);
        } catch (IOException e) {
            e.printStackTrace();
        }
        keys.put(key, path);
    }

    private void publishFileSystemEvent(Path resolvedFilePath) {
        logger.info("Send archive file to kafka consumer");
        publishFileSystemChange(from(resolvedFilePath));
    }

    public void publishFileSystemChange(ArchiveMetaData archiveMetaData) {
        logger.info("Preparing Message to send to kafka -- {}", archiveMetaData.getName());
        ProducerRecord<String,String> record = null;
        try {
            record = new ProducerRecord<>(
                    SYSTEM_CHANGE_TOPIC,
                    new ObjectMapper().writeValueAsString(archiveMetaData));
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
        kafkaProducer.send(record,(data,exception) -> {
            logger.info("Message Sent to offset {}, for file {}", data.offset(),archiveMetaData.getName());
        });
    }

    private ArchiveMetaData from(Path resolvedPath) {
        try {
            BasicFileAttributes fileAttributes = Files.readAttributes(
                    resolvedPath, BasicFileAttributes.class, LinkOption.NOFOLLOW_LINKS);
            return new ArchiveMetaData(
                    resolvedPath.toFile().getName(),
                    LocalDateTime.ofInstant(fileAttributes.creationTime().toInstant(), ZoneId.systemDefault()),
                    fileAttributes.size(), fileAttributes.isDirectory(),
                    resolvedPath.toString(), resolvedPath.getParent().toString());
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }
}
