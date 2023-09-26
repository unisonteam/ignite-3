package org.apache.ignite.compute.v2;

import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import org.apache.ignite.compute.DeploymentUnit;

public interface AsyncExecutorBuilder<T> {

    AsyncExecutorBuilder<T> deploymentUnits(DeploymentUnit... units);

    AsyncExecutorBuilder<T> colocated(Consumer<Colocator> colocator);

    AsyncExecutorBuilder<T> typeMapper(String className);

    Executor<CompletableFuture<T>> build();

    default CompletableFuture<T> call(Object... args) {
        return build().call(args);
    }
}
