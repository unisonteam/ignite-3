package org.apache.ignite.compute.v2;

import java.util.function.Consumer;
import org.apache.ignite.compute.DeploymentUnit;

public interface ExecutorBuilder<T> {

    ExecutorBuilder<T> deploymentUnits(DeploymentUnit... units);

    ExecutorBuilder<T> typeMapper(String className);

    ExecutorBuilder<T> colocated(Consumer<Colocator> colocator);

    AsyncExecutorBuilder<T> async();

    Executor<T> build();

    default T call(Object... args) {
        return build().call(args);
    }


    static <R> ExecutorBuilder<R> empty() {
        return new ExecutorBuilder<>() {
            @Override
            public ExecutorBuilder<R> deploymentUnits(DeploymentUnit... units) {
                return null;
            }

            @Override
            public ExecutorBuilder<R> colocated(Consumer<Colocator> colocator) {
                return null;
            }

            @Override
            public AsyncExecutorBuilder<R> async() {
                return null;
            }

            @Override
            public Executor<R> build() {
                return null;
            }
        };
    }
}
