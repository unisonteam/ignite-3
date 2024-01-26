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
import static org.apache.ignite.internal.compute.ComputeUtils.taskClass;

import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
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
import java.util.stream.Collectors;
import org.apache.ignite.compute.DeploymentUnit;
import org.apache.ignite.compute.IgniteCompute;
import org.apache.ignite.compute.JobExecution;
import org.apache.ignite.compute.JobStatus;
import org.apache.ignite.compute.TaskExecution;
import org.apache.ignite.compute.task.ComputeTask;
import org.apache.ignite.compute.task.TaskOptions;
import org.apache.ignite.internal.compute.loader.JobContextManager;
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

    private final JobContextManager jobContextManager;

    /**
     * Create new instance.
     */
    public IgniteComputeImpl(
            TopologyService topologyService,
            IgniteTablesInternal tables,
            ComputeComponent computeComponent,
            JobContextManager jobContextManager
    ) {
        this.topologyProvider = new TopologyProvider() {
            @Override
            public ClusterNode localMember() {
                return topologyService.localMember();
            }

            @Override
            public Collection<ClusterNode> allMembers() {
                return topologyService.allMembers();
            }
        };
        this.tables = tables;
        this.computeComponent = computeComponent;
        this.jobContextManager = jobContextManager;
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

        ComputeTask<R> computeTask = jobContextManager.acquireClassLoader(units)
                .thenApply(context -> ComputeUtils.<R>instantiateTask(context.classLoader(), taskClassName))
                .join();

        Map<String, List<TaskOptions>> result = computeTask.map(topologyProvider, args);

        Map<JobExecution<Object>, Set<ClusterNode>> executions = new HashMap<>();

        for (Entry<String, List<TaskOptions>> entry : result.entrySet()) {
            String jobClassName = entry.getKey();
            List<TaskOptions> options = entry.getValue();
            for (TaskOptions option : options) {
                JobExecution<Object> jobExecution = executeAsync(
                        option.nodes(),
                        option.units(),
                        jobClassName,
                        option.args()
                );
                executions.put(jobExecution, option.nodes());
            }
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
