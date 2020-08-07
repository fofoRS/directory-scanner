package org.quarkus.reactive;

import io.vertx.mutiny.core.Vertx;

import javax.annotation.PostConstruct;
import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;

import static org.quarkus.reactive.WorkerVerticle.FILE_SYSTEM_EVENT_ADDRESS;

@ApplicationScoped
public class DispatcherVerticle{

    @Inject
    Vertx vertx;

    public void dispatchFileSystemWatcher() {
        vertx
                .eventBus()
                .sendAndForget(
                        FILE_SYSTEM_EVENT_ADDRESS,
                        "init");
    }
}
