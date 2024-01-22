/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.compute;

import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toMap;
import static java.util.stream.Collectors.toUnmodifiableMap;

import java.lang.reflect.InvocationTargetException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.ComputeTask;
import org.apache.ignite.compute.DeploymentUnit;
import org.apache.ignite.compute.IgniteCompute;
import org.apache.ignite.compute.JobExecution;
import org.apache.ignite.compute.JobExecution;
import org.apache.ignite.compute.JobStatus;
import org.apache.ignite.compute.TaskExecution;
import org.apache.ignite.internal.lang.IgniteInternalException;
import org.apache.ignite.internal.table.IgniteTablesInternal;
import org.apache.ignite.internal.table.TableViewInternal;
import org.apache.ignite.internal.util.ExceptionUtils;
import org.apache.ignite.lang.ErrorGroups.Common;
import org.apache.ignite.lang.TableNotFoundException;
import org.apache.ignite.lang.util.IgniteNameUtils;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.network.TopologyProvider;
import org.apache.ignite.network.TopologyService;
import org.apache.ignite.table.Tuple;
import org.apache.ignite.table.mapper.Mapper;

/**
 * Implementation of {@link IgniteCompute}.
 */
public class IgniteComputeImpl implements IgniteCompute {
    private static final String DEFAULT_SCHEMA_NAME = "PUBLIC";

    private final TopologyProvider topologyProvider;
    private final IgniteTablesInternal tables;

    private final ComputeComponent computeComponent;

    private final ThreadLocalRandom random = ThreadLocalRandom.current();

    private final NodeLeftEventsSource nodeLeftEventsSource;

    /**
     * Create new instance.
     */
    public IgniteComputeImpl(TopologyProvider topologyProvider, IgniteTablesInternal tables, ComputeComponent computeComponent) {
        this.topologyProvider = topologyProvider;
        this.tables = tables;
        this.computeComponent = computeComponent;
        this.nodeLeftEventsSource = new NodeLeftEventsSource(topologyService);
    }

    /** {@inheritDoc} */
    @Override
    public <R> JobExecution<R> executeAsync(Set<ClusterNode> nodes, List<DeploymentUnit> units, String jobClassName, Object... args) {
        Objects.requireNonNull(nodes);
        Objects.requireNonNull(units);
        Objects.requireNonNull(jobClassName);

        if (nodes.isEmpty()) {
            throw new IllegalArgumentException("nodes must not be empty.");
        }

        Set<ClusterNode> candidates = new HashSet<>(nodes);
        ClusterNode targetNode = randomNode(candidates);
        candidates.remove(targetNode);

        return new JobExecutionWrapper<>(executeOnOneNodeWithFailover(targetNode, candidates, units, jobClassName, args));
    }

    /** {@inheritDoc} */
    @Override
    public <R> R execute(
            Set<ClusterNode> nodes,
            List<DeploymentUnit> units,
            String jobClassName,
            Object... args
    ) {
        try {
            return this.<R>executeAsync(nodes, units, jobClassName, args).resultAsync().join();
        } catch (CompletionException e) {
            throw ExceptionUtils.sneakyThrow(ExceptionUtils.copyExceptionWithCause(e));
        }
    }

    private ClusterNode randomNode(Set<ClusterNode> nodes) {
        int nodesToSkip = random.nextInt(nodes.size());

        Iterator<ClusterNode> iterator = nodes.iterator();
        for (int i = 0; i < nodesToSkip; i++) {
            iterator.next();
        }

        return iterator.next();
    }

    private <R> JobExecution<R> executeOnOneNodeWithFailover(
            ClusterNode targetNode,
            Set<ClusterNode> failoverCandidates,
            List<DeploymentUnit> units,
            String jobClassName,
            Object[] args
    ) {
        if (isLocal(targetNode)) {
            return computeComponent.executeLocally(units, jobClassName, args);
        } else {
            return new ComputeJobFailover<R>(
                    computeComponent, nodeLeftEventsSource,
                    targetNode, failoverCandidates, units,
                    jobClassName, args
            ).failSafeExecute();
        }
    }

    private <R> JobExecution<R> executeOnOneNode(
            ClusterNode targetNode,
            List<DeploymentUnit> units,
            String jobClassName,
            Object[] args
    ) {
        if (isLocal(targetNode)) {
            return computeComponent.executeLocally(units, jobClassName, args);
        } else {
            return computeComponent.executeRemotely(targetNode, units, jobClassName, args);
        }
    }

    private boolean isLocal(ClusterNode targetNode) {
        return targetNode.equals(topologyProvider.localMember());
    }

    /** {@inheritDoc} */
    @Override
    public <R> JobExecution<R> executeColocatedAsync(
            String tableName,
            Tuple key,
            List<DeploymentUnit> units,
            String jobClassName,
            Object... args
    ) {
        Objects.requireNonNull(tableName);
        Objects.requireNonNull(key);
        Objects.requireNonNull(units);
        Objects.requireNonNull(jobClassName);

        return new JobExecutionFutureWrapper<>(
                requiredTable(tableName)
                        .thenApply(table -> leaderOfTablePartitionByTupleKey(table, key))
                        .thenApply(primaryNode -> executeOnOneNode(primaryNode, units, jobClassName, args))
        );
    }

    /** {@inheritDoc} */
    @Override
    public <K, R> JobExecution<R> executeColocatedAsync(
            String tableName,
            K key,
            Mapper<K> keyMapper,
            List<DeploymentUnit> units,
            String jobClassName,
            Object... args
    ) {
        Objects.requireNonNull(tableName);
        Objects.requireNonNull(key);
        Objects.requireNonNull(keyMapper);
        Objects.requireNonNull(units);
        Objects.requireNonNull(jobClassName);

        return new JobExecutionFutureWrapper<>(requiredTable(tableName)
                .thenApply(table -> leaderOfTablePartitionByMappedKey(table, key, keyMapper))
                .thenApply(primaryNode -> executeOnOneNode(primaryNode, units, jobClassName, args)));
    }

    /** {@inheritDoc} */
    @Override
    public <R> R executeColocated(
            String tableName,
            Tuple key,
            List<DeploymentUnit> units,
            String jobClassName,
            Object... args
    ) {
        try {
            return this.<R>executeColocatedAsync(tableName, key, units, jobClassName, args).resultAsync().join();
        } catch (CompletionException e) {
            throw ExceptionUtils.sneakyThrow(ExceptionUtils.copyExceptionWithCause(e));
        }
    }

    /** {@inheritDoc} */
    @Override
    public <K, R> R executeColocated(
            String tableName,
            K key,
            Mapper<K> keyMapper,
            List<DeploymentUnit> units,
            String jobClassName,
            Object... args
    ) {
        try {
            return this.<K, R>executeColocatedAsync(tableName, key, keyMapper, units, jobClassName, args).resultAsync().join();
        } catch (CompletionException e) {
            throw ExceptionUtils.sneakyThrow(ExceptionUtils.copyExceptionWithCause(e));
        }
    }

    private CompletableFuture<TableViewInternal> requiredTable(String tableName) {
        String parsedName = IgniteNameUtils.parseSimpleName(tableName);

        return tables.tableViewAsync(parsedName)
                .thenApply(table -> {
                    if (table == null) {
                        throw new TableNotFoundException(DEFAULT_SCHEMA_NAME, parsedName);
                    }
                    return table;
                });
    }

    private static ClusterNode leaderOfTablePartitionByTupleKey(TableViewInternal table, Tuple key) {
        return requiredLeaderByPartition(table, table.partition(key));
    }

    private static  <K> ClusterNode leaderOfTablePartitionByMappedKey(TableViewInternal table, K key, Mapper<K> keyMapper) {
        return requiredLeaderByPartition(table, table.partition(key, keyMapper));
    }

    private static ClusterNode requiredLeaderByPartition(TableViewInternal table, int partitionIndex) {
        ClusterNode leaderNode = table.leaderAssignment(partitionIndex);
        if (leaderNode == null) {
            throw new IgniteInternalException(Common.INTERNAL_ERR, "Leader not found for partition " + partitionIndex);
        }

        return leaderNode;
    }

    /** {@inheritDoc} */
    @Override
    public <R> Map<ClusterNode, JobExecution<R>> broadcastAsync(
            Set<ClusterNode> nodes,
            List<DeploymentUnit> units,
            String jobClassName,
            Object... args
    ) {
        Objects.requireNonNull(nodes);
        Objects.requireNonNull(units);
        Objects.requireNonNull(jobClassName);

        return nodes.stream()
                .collect(toUnmodifiableMap(identity(),
                        // No failover nodes for broadcast. We use failover here in order to complete futures with exceptions
                        // if worker node has left the cluster.
                        node -> new JobExecutionWrapper<>(executeOnOneNodeWithFailover(node, Set.of(), units, jobClassName, args))));
    }

    @Override
    public <R> TaskExecution<R> mapReduceAsync(
            List<DeploymentUnit> units,
            String taskClassName,
            Object... args
    ) {
        Objects.requireNonNull(units);
        Objects.requireNonNull(taskClassName);

        try {
            Class<? extends ComputeTask<R>> taskClass = (Class<? extends ComputeTask<R>>) Class.forName(taskClassName);

            ComputeTask<R> computeTask = taskClass.getDeclaredConstructor().newInstance();

            Map<ComputeJob<?>, ClusterNode> result = computeTask.map(topologyProvider, args);

            Map<JobExecution<Object>, ClusterNode> executions = result.entrySet().stream()
                    .collect(toMap(
                            entry -> executeOnOneNode(entry.getValue(), units, entry.getKey().getClass().getName(), args),
                            e -> e.getValue()
                    ));

            return new TaskExecution<>() {
                @Override
                public CompletableFuture<R> resultAsync() {
                    Map<ClusterNode, CompletableFuture<?>> collect = executions.entrySet().stream()
                            .collect(toMap(Entry::getKey, e -> e.getValue().resultAsync()));

                    CompletableFuture<R> result = new CompletableFuture<>();

                    CompletableFuture.allOf(collect.values().toArray(CompletableFuture[]::new)).whenComplete(
                            (unused, throwable) -> {
                                if (throwable != null) {
                                    result.completeExceptionally(throwable);
                                } else {
                                    result.complete(computeTask.reduce(collect.entrySet().stream()
                                            .collect(toMap(Entry::getKey, e -> e.getValue().join())))
                                    );
                                }
                            });

                    return result;
                }

                @Override
                public CompletableFuture<Map<JobStatus, ClusterNode>> statusesAsync() {
                    Map<ClusterNode, CompletableFuture<JobStatus>> collect = executions.entrySet().stream()
                            .collect(toMap(Entry::getKey, e -> e.getValue().statusAsync()));

                    CompletableFuture<Map<ClusterNode, JobStatus>> result = new CompletableFuture<>();

                    CompletableFuture.allOf(collect.values().toArray(CompletableFuture[]::new)).whenComplete(
                            (unused, throwable) -> {
                                if (throwable != null) {
                                    result.completeExceptionally(throwable);
                                } else {
                                    result.complete(collect.entrySet().stream()
                                            .collect(toMap(Entry::getKey, e -> e.getValue().join()))
                                    );
                                }
                            });

                    return result;
                }

                @Override
                public CompletableFuture<Void> cancelAsync() {
                    CompletableFuture<?>[] arr = new CompletableFuture[executions.size()];
                    for (int i = 0, collectSize = executions.size(); i < collectSize; i++) {
                        JobExecution<Object> jobExecution = executions.get(i);
                        arr[i] = jobExecution.cancelAsync();
                    }

                    return CompletableFuture.allOf(arr);
                }
            };

        } catch (ClassNotFoundException
                 | NoSuchMethodException
                 | InstantiationException
                 | IllegalAccessException
                 | InvocationTargetException e) {
            throw new RuntimeException(e);
        }


    }
}
