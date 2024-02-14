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

import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toMap;
import static java.util.stream.Collectors.toUnmodifiableMap;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.compute.ComputeException;
import org.apache.ignite.compute.DeploymentUnit;
import org.apache.ignite.compute.IgniteCompute;
import org.apache.ignite.compute.JobExecution;
import org.apache.ignite.compute.JobExecutionOptions;
import org.apache.ignite.compute.JobStatus;
import org.apache.ignite.compute.TaskExecution;
import org.apache.ignite.compute.task.ComputeTask;
import org.apache.ignite.compute.task.JobExecutionParameters;
import org.apache.ignite.compute.task.PartitionProvider;
import org.apache.ignite.internal.compute.loader.JobContextManager;
import org.apache.ignite.internal.hlc.HybridClock;
import org.apache.ignite.internal.placementdriver.PlacementDriver;
import org.apache.ignite.internal.replicator.TablePartitionId;
import org.apache.ignite.internal.table.IgniteTablesInternal;
import org.apache.ignite.internal.table.TableViewInternal;
import org.apache.ignite.internal.util.CompletableFutures;
import org.apache.ignite.internal.util.ExceptionUtils;
import org.apache.ignite.lang.ErrorGroups.Compute;
import org.apache.ignite.lang.TableNotFoundException;
import org.apache.ignite.lang.util.IgniteNameUtils;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.network.TopologyProvider;
import org.apache.ignite.network.TopologyService;
import org.apache.ignite.table.Tuple;
import org.apache.ignite.table.mapper.Mapper;
import org.jetbrains.annotations.Nullable;

/**
 * Implementation of {@link IgniteCompute}.
 */
public class IgniteComputeImpl implements IgniteComputeInternal {
    private static final String DEFAULT_SCHEMA_NAME = "PUBLIC";

    private final TopologyProvider topologyProvider;

    private final TopologyService topologyService;

    private final IgniteTablesInternal tables;

    private final ComputeComponent computeComponent;

    private final ThreadLocalRandom random = ThreadLocalRandom.current();

    private final PlacementDriver placementDriver;

    private final HybridClock clock;

    private final JobContextManager jobContextManager;

    /**
     * Create new instance.
     */
    public IgniteComputeImpl(
            PlacementDriver placementDriver,
            TopologyService topologyService,
            IgniteTablesInternal tables,
            JobContextManager jobContextManager,
            ComputeComponent computeComponent,
            HybridClock clock
    ) {
        this.placementDriver = placementDriver;
        this.topologyService = topologyService;
        this.tables = tables;
        this.computeComponent = computeComponent;
        this.jobContextManager = jobContextManager;
        this.clock = clock;
        this.topologyProvider = new TopologyProvider() {
            @Override
            public ClusterNode localMember() {
                return IgniteComputeImpl.this.topologyService.localMember();
            }

            @Override
            public Collection<ClusterNode> allMembers() {
                return IgniteComputeImpl.this.topologyService.allMembers();
            }
        };
    }

    /** {@inheritDoc} */
    @Override
    public <R> JobExecution<R> executeAsync(
            Set<ClusterNode> nodes,
            List<DeploymentUnit> units,
            String jobClassName,
            JobExecutionOptions options,
            Object... args
    ) {
        Objects.requireNonNull(nodes);
        Objects.requireNonNull(units);
        Objects.requireNonNull(jobClassName);
        Objects.requireNonNull(options);

        if (nodes.isEmpty()) {
            throw new IllegalArgumentException("nodes must not be empty.");
        }

        return executeAsyncWithFailover(nodes, units, jobClassName, options, args);
    }

    @Override
    public <R> JobExecution<R> executeAsyncWithFailover(
            Set<ClusterNode> nodes,
            List<DeploymentUnit> units,
            String jobClassName,
            JobExecutionOptions options,
            Object... args
    ) {
        Set<ClusterNode> candidates = new HashSet<>(nodes);
        ClusterNode targetNode = randomNode(candidates);
        candidates.remove(targetNode);

        NextWorkerSelector selector = new DeqNextWorkerSelector(new ConcurrentLinkedDeque<>(candidates));

        return new JobExecutionWrapper<>(
                executeOnOneNodeWithFailover(
                        targetNode,
                        selector,
                        units,
                        jobClassName,
                        options,
                        args
                ));
    }

    /** {@inheritDoc} */
    @Override
    public <R> R execute(
            Set<ClusterNode> nodes,
            List<DeploymentUnit> units,
            String jobClassName,
            JobExecutionOptions options,
            Object... args
    ) {
        try {
            return this.<R>executeAsync(nodes, units, jobClassName, options, args).resultAsync().join();
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
            NextWorkerSelector nextWorkerSelector,
            List<DeploymentUnit> units,
            String jobClassName,
            JobExecutionOptions jobExecutionOptions,
            Object[] args
    ) {
        ExecutionOptions options = ExecutionOptions.from(jobExecutionOptions);
        if (isLocal(targetNode)) {
            return computeComponent.executeLocally(options, units, jobClassName, args);
        } else {
            return computeComponent.executeRemotelyWithFailover(targetNode, nextWorkerSelector, units, jobClassName, options, args);
        }
    }

    private static class DeqNextWorkerSelector implements NextWorkerSelector {
        private final ConcurrentLinkedDeque<ClusterNode> deque;

        private DeqNextWorkerSelector(ConcurrentLinkedDeque<ClusterNode> deque) {
            this.deque = deque;
        }

        @Override
        public CompletableFuture<ClusterNode> next() {
            try {
                return completedFuture(deque.pop());
            } catch (NoSuchElementException ex) {
                return nullCompletedFuture();
            }
        }
    }

    private boolean isLocal(ClusterNode targetNode) {
        return targetNode.equals(topologyProvider.localMember());
    }

    /** {@inheritDoc} */
    @Override
    public <R> JobExecution<R> executeColocatedAsync(
            String tableName,
            Tuple tuple,
            List<DeploymentUnit> units,
            String jobClassName,
            JobExecutionOptions options,
            Object... args
    ) {
        Objects.requireNonNull(tableName);
        Objects.requireNonNull(tuple);
        Objects.requireNonNull(units);
        Objects.requireNonNull(jobClassName);
        Objects.requireNonNull(options);

        return new JobExecutionFutureWrapper<>(
                requiredTable(tableName)
                        .thenCompose(table -> primaryReplicaForPartitionByTupleKey(table, tuple)
                                .thenApply(primaryNode -> executeOnOneNodeWithFailover(
                                        primaryNode,
                                        new NextColocatedWorkerSelector<>(placementDriver, topologyService, clock, table, tuple),
                                        units, jobClassName, options, args
                                )))
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
            JobExecutionOptions options,
            Object... args
    ) {
        Objects.requireNonNull(tableName);
        Objects.requireNonNull(key);
        Objects.requireNonNull(keyMapper);
        Objects.requireNonNull(units);
        Objects.requireNonNull(jobClassName);
        Objects.requireNonNull(options);

        return new JobExecutionFutureWrapper<>(
                requiredTable(tableName)
                        .thenCompose(table -> primaryReplicaForPartitionByMappedKey(table, key, keyMapper)
                                .thenApply(primaryNode -> executeOnOneNodeWithFailover(
                                        primaryNode,
                                        new NextColocatedWorkerSelector<>(placementDriver, topologyService, clock, table, key, keyMapper),
                                        units, jobClassName, options, args
                                )))
        );
    }

    /** {@inheritDoc} */
    @Override
    public <R> R executeColocated(
            String tableName,
            Tuple key,
            List<DeploymentUnit> units,
            String jobClassName,
            JobExecutionOptions options,
            Object... args
    ) {
        try {
            return this.<R>executeColocatedAsync(tableName, key, units, jobClassName, options, args).resultAsync().join();
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
            JobExecutionOptions options,
            Object... args
    ) {
        try {
            return this.<K, R>executeColocatedAsync(tableName, key, keyMapper, units, jobClassName, options, args).resultAsync()
                    .join();
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

    private CompletableFuture<ClusterNode> primaryReplicaForPartitionByTupleKey(TableViewInternal table, Tuple key) {
        return primaryReplicaForPartition(table, table.partition(key));
    }

    private <K> CompletableFuture<ClusterNode> primaryReplicaForPartitionByMappedKey(TableViewInternal table, K key,
            Mapper<K> keyMapper) {
        return primaryReplicaForPartition(table, table.partition(key, keyMapper));
    }

    private CompletableFuture<ClusterNode> primaryReplicaForPartition(TableViewInternal table, int partitionIndex) {
        TablePartitionId tablePartitionId = new TablePartitionId(table.tableId(), partitionIndex);

        return placementDriver.awaitPrimaryReplica(tablePartitionId, clock.now(), 30, TimeUnit.SECONDS)
                .thenApply(replicaMeta -> {
                    if (replicaMeta != null && replicaMeta.getLeaseholderId() != null) {
                        return topologyService.getById(replicaMeta.getLeaseholderId());
                    }

                    throw new ComputeException(
                            Compute.PRIMARY_REPLICA_RESOLVE_ERR,
                            "Can not find primary replica for [table=" + table.name() + ", partition=" + partitionIndex + "]."
                    );
                });
    }

    /** {@inheritDoc} */
    @Override
    public <R> Map<ClusterNode, JobExecution<R>> broadcastAsync(
            Set<ClusterNode> nodes,
            List<DeploymentUnit> units,
            String jobClassName,
            JobExecutionOptions options,
            Object... args
    ) {
        Objects.requireNonNull(nodes);
        Objects.requireNonNull(units);
        Objects.requireNonNull(jobClassName);
        Objects.requireNonNull(options);

        return nodes.stream()
                .collect(toUnmodifiableMap(identity(),
                        // No failover nodes for broadcast. We use failover here in order to complete futures with exceptions
                        // if worker node has left the cluster.
                        node -> new JobExecutionWrapper<>(executeOnOneNodeWithFailover(node,
                                CompletableFutures::nullCompletedFuture, units, jobClassName, options, args))));
    }

    @Override
    public CompletableFuture<Collection<JobStatus>> statusesAsync() {
        return computeComponent.statusesAsync();
    }

    @Override
    public CompletableFuture<@Nullable JobStatus> statusAsync(UUID jobId) {
        return computeComponent.statusAsync(jobId);
    }

    @Override
    public CompletableFuture<@Nullable Boolean> cancelAsync(UUID jobId) {
        return computeComponent.cancelAsync(jobId);
    }

    @Override
    public CompletableFuture<@Nullable Boolean> changePriorityAsync(UUID jobId, int newPriority) {
        return computeComponent.changePriorityAsync(jobId, newPriority);
    }

    @Override
    public <R> TaskExecution<R> mapReduceAsync(
            List<DeploymentUnit> units,
            String taskClassName,
            Object... args
    ) {
        Objects.requireNonNull(units);
        Objects.requireNonNull(taskClassName);

        ComputeTask<R> computeTask = jobContextManager.acquireClassLoader(units)
                .thenApply(context -> ComputeUtils.<R>instantiateTask(context.classLoader(), taskClassName))
                .join();

        List<JobExecutionParameters> result = computeTask.map(topologyProvider, tableName -> {

        }, args);

        Map<JobExecution<Object>, Set<ClusterNode>> executions = new HashMap<>();

        for (JobExecutionParameters params : result) {
            String jobClassName = params.jobClassName();
            JobExecution<Object> jobExecution = executeAsync(
                    params.nodes(),
                    params.units(),
                    jobClassName,
                    params.args()
            );
            executions.put(jobExecution, params.nodes());
        }

        return new TaskExecution<>() {

            @Override
            public CompletableFuture<R> resultAsync() {
                CompletableFuture<?>[] collect = executions.keySet().stream()
                        .map(JobExecution::resultAsync).toArray(CompletableFuture[]::new);

                CompletableFuture<R> result = new CompletableFuture<>();

                CompletableFuture.allOf(collect).whenComplete(
                        (unused, throwable) -> {
                            if (throwable != null) {
                                result.completeExceptionally(throwable);
                            } else {
                                result.complete(computeTask.reduce(executions.entrySet().stream()
                                        .collect(toMap(e -> e.getKey().idAsync().join(),
                                                e -> e.getKey().resultAsync().join())
                                        )
                                ));
                            }
                        });

                return result;
            }

            @Override
            public CompletableFuture<Map<ClusterNode, JobStatus>> statusesAsync() {
                Map<CompletableFuture<JobStatus>, ClusterNode> collect = executions.entrySet().stream()
                        .collect(toMap(e -> e.getKey().statusAsync(), e -> e.getValue().iterator().next()));

                CompletableFuture<Map<ClusterNode, JobStatus>> result = new CompletableFuture<>();

                CompletableFuture.allOf(collect.keySet().toArray(CompletableFuture[]::new)).whenComplete(
                        (unused, throwable) -> {
                            if (throwable != null) {
                                result.completeExceptionally(throwable);
                            } else {
                                result.complete(collect.entrySet().stream()
                                        .collect(toMap(Entry::getValue, e -> e.getKey().join()))
                                );
                            }
                        });

                return result;
            }

            @Override
            public CompletableFuture<Void> cancelAsync() {
                CompletableFuture<?>[] arr = new CompletableFuture[executions.size()];
                int i = 0;
                for (JobExecution<Object> jobExecution : executions.keySet()) {
                    arr[i] = jobExecution.cancelAsync();
                    i++;
                }

                return CompletableFuture.allOf(arr);
            }
        };
    }
}
