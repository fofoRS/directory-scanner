package org.quarkus.reactive;

import io.quarkus.runtime.StartupEvent;
import io.vertx.core.DeploymentOptions;
import io.vertx.mutiny.core.Vertx;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.event.Observes;
import javax.inject.Inject;
import java.util.concurrent.TimeUnit;

@ApplicationScoped
public class VerticalsDeployer {

    Logger logger = LoggerFactory.getLogger(VerticalsDeployer.class);

    @Inject
    Vertx vertx;
    @Inject
    DispatcherVerticle dispatcherVerticle;

    public void onStart(@Observes StartupEvent event,WorkerVerticle workerVerticle) {
        DeploymentOptions options =
                new DeploymentOptions()
                        .setWorkerPoolName("file.system.watcher.pool.worker")
                        .setWorkerPoolSize(5)
                        .setWorker(true)
                .setMaxWorkerExecuteTime(10000)
                .setMaxWorkerExecuteTimeUnit(TimeUnit.SECONDS);
        vertx
                .deployVerticle(workerVerticle,options)
                .subscribe()
                .with(e -> {
                    if(!e.isEmpty()) {
                        dispatcherVerticle.dispatchFileSystemWatcher();
                    }
                });
    }
}
