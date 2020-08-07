package org.quarkus.reactive;

import io.vertx.core.AbstractVerticle;

import javax.enterprise.context.ApplicationScoped;

@ApplicationScoped
public class WorkerVerticle extends AbstractVerticle {

    static final String FILE_SYSTEM_EVENT_ADDRESS = "file.system.events";

    @Override
    public void start() throws Exception {
        // This can be implemented as Vertx Service.
        DirectoryEventProducer directoryEventProducer = new DirectoryEventProducer();
        vertx
                .eventBus()
                .consumer(FILE_SYSTEM_EVENT_ADDRESS,message -> {
                    String payload = (String)message.body();
                    directoryEventProducer.watch(payload);
                });
    }
}
