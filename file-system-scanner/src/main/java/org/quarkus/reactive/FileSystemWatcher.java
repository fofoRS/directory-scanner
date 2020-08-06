package org.quarkus.reactive;

import io.quarkus.runtime.Startup;
import io.vertx.mutiny.core.eventbus.EventBus;
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

import static java.nio.file.StandardWatchEventKinds.*;

@ApplicationScoped
@Startup
public class FileSystemWatcher {

    Logger logger = LoggerFactory.getLogger(FileSystemWatcher.class);

    static final Map<WatchKey, Path> keys = new HashMap<>();
    static final String directory = "/Users/rodolfo/Documents/personal/training/learning/quarkus/quarkus-file-system-scanner/file-system-scanner/testDir";
    static final String FILE_SYSTEM_EVENT_ADDRESS = "file.system.events";

    WatchService watcher;

    @Inject
    EventBus eventBus;

    @PostConstruct
    public void init() throws IOException {
        watcher = FileSystems.getDefault().newWatchService();
        registerPath(Paths.get(directory));
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

    private void processFileSystemEvent(Path currentDir,WatchEvent<Path> watchedEvent,
                                        WatchEvent.Kind<Path> eventKind) {
        Path filePath = watchedEvent.context();
        Path child = currentDir.resolve(filePath);
        if(watchedEvent.count() > 1) {
            logger.info("Repeated Event --- skip archive processing");
            return;
        }
        System.out.format("Logging Event data  %s: %s\n", eventKind.name(), child);
        publishFileSystemChange(child);
        registerPath(child);
    }

    private void registerPath(Path path) {
        WatchKey key = null;
        try {
            logger.info("Registering new Archive {}", path.toFile().getName());
            if(Files.isDirectory(path)) {
                key = path.register(watcher, OVERFLOW, ENTRY_CREATE, ENTRY_DELETE, ENTRY_MODIFY);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        keys.put(key, path);
    }

    private void publishFileSystemChange(Path resolvedFilePath) {
        logger.info("Send archive file to kafka consumer");
        eventBus.sendAndForget(FILE_SYSTEM_EVENT_ADDRESS,resolvedFilePath.toFile().getName());
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
