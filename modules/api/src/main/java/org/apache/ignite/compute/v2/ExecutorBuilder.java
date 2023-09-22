package org.apache.ignite.compute.v2;

import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import org.apache.ignite.compute.DeploymentUnit;
import org.apache.ignite.network.ClusterNode;

public interface ExecutorBuilder<T> {

    ExecutorBuilder<T> deploymentUnits(List<DeploymentUnit> units);

    ExecutorBuilder<T> nodes(Set<ClusterNode> nodes);

    ExecutorBuilder<T> colocated(Consumer<ColocatorBuilder> colocator);

    ExecutorBuilder<T> sync();

    Executor<T> build();
}
