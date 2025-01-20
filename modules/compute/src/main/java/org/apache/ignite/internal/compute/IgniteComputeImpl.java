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
import static java.util.stream.Collectors.toList;
import static org.apache.ignite.internal.lang.IgniteExceptionMapperUtil.mapToPublicException;
import static org.apache.ignite.internal.util.CompletableFutures.allOfToList;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;
import static org.apache.ignite.internal.util.ExceptionUtils.unwrapCause;
import static org.apache.ignite.lang.ErrorGroups.Compute.COMPUTE_JOB_FAILED_ERR;

import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.ignite.compute.AllNodesBroadcastJobTarget;
import org.apache.ignite.compute.AnyNodeJobTarget;
import org.apache.ignite.compute.BroadcastExecution;
import org.apache.ignite.compute.BroadcastJobTarget;
import org.apache.ignite.compute.ColocatedJobTarget;
import org.apache.ignite.compute.ComputeException;
import org.apache.ignite.compute.IgniteCompute;
import org.apache.ignite.compute.JobDescriptor;
import org.apache.ignite.compute.JobExecution;
import org.apache.ignite.compute.JobExecutionOptions;
import org.apache.ignite.compute.JobState;
import org.apache.ignite.compute.JobTarget;
import org.apache.ignite.compute.NodeNotFoundException;
import org.apache.ignite.compute.TableJobTarget;
import org.apache.ignite.compute.TaskDescriptor;
import org.apache.ignite.compute.task.MapReduceJob;
import org.apache.ignite.compute.task.TaskExecution;
import org.apache.ignite.deployment.DeploymentUnit;
import org.apache.ignite.internal.client.proto.StreamerReceiverSerializer;
import org.apache.ignite.internal.compute.streamer.StreamerReceiverJob;
import org.apache.ignite.internal.hlc.HybridClock;
import org.apache.ignite.internal.network.TopologyService;
import org.apache.ignite.internal.placementdriver.PlacementDriver;
import org.apache.ignite.internal.replicator.TablePartitionId;
import org.apache.ignite.internal.sql.SqlCommon;
import org.apache.ignite.internal.sql.engine.api.kill.CancellableOperationType;
import org.apache.ignite.internal.sql.engine.api.kill.OperationKillHandler;
import org.apache.ignite.internal.table.IgniteTablesInternal;
import org.apache.ignite.internal.table.StreamerReceiverRunner;
import org.apache.ignite.internal.table.TableViewInternal;
import org.apache.ignite.internal.table.partition.HashPartition;
import org.apache.ignite.internal.util.CompletableFutures;
import org.apache.ignite.internal.util.ExceptionUtils;
import org.apache.ignite.lang.CancelHandleHelper;
import org.apache.ignite.lang.CancellationToken;
import org.apache.ignite.lang.ErrorGroups.Compute;
import org.apache.ignite.lang.IgniteException;
import org.apache.ignite.lang.TableNotFoundException;
import org.apache.ignite.lang.util.IgniteNameUtils;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.table.ReceiverDescriptor;
import org.apache.ignite.table.Tuple;
import org.apache.ignite.table.mapper.Mapper;
import org.apache.ignite.table.partition.Partition;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

/**
 * Implementation of {@link IgniteCompute}.
 */
public class IgniteComputeImpl implements IgniteComputeInternal, StreamerReceiverRunner {
    private final TopologyService topologyService;

    private final IgniteTablesInternal tables;

    private final ComputeComponent computeComponent;

    private final PlacementDriver placementDriver;

    private final HybridClock clock;

    /**
     * Create new instance.
     */
    public IgniteComputeImpl(PlacementDriver placementDriver, TopologyService topologyService,
            IgniteTablesInternal tables, ComputeComponent computeComponent,
            HybridClock clock) {
        this.placementDriver = placementDriver;
        this.topologyService = topologyService;
        this.tables = tables;
        this.computeComponent = computeComponent;
        this.clock = clock;

        tables.setStreamerReceiverRunner(this);
    }

    @Override
    public <T, R> CompletableFuture<JobExecution<R>> submitAsync(
            JobTarget target,
            JobDescriptor<T, R> descriptor,
            @Nullable T args,
            @Nullable CancellationToken cancellationToken
    ) {
        Objects.requireNonNull(target);
        Objects.requireNonNull(descriptor);

        ComputeJobDataHolder argHolder = SharedComputeUtils.marshalArgOrResult(args, descriptor.argumentMarshaller());

        if (target instanceof AnyNodeJobTarget) {
            Set<ClusterNode> nodes = ((AnyNodeJobTarget) target).nodes();

            return completedFuture(unmarshalResult(
                    executeAsyncWithFailover(
                            nodes, descriptor.units(), descriptor.jobClassName(), descriptor.options(), argHolder, cancellationToken
                    ),
                    descriptor
            ));
        }

        if (target instanceof ColocatedJobTarget) {
            ColocatedJobTarget colocatedTarget = (ColocatedJobTarget) target;
            var mapper = (Mapper<? super Object>) colocatedTarget.keyMapper();
            String tableName = colocatedTarget.tableName();
            Object key = colocatedTarget.key();

            CompletableFuture<JobExecution<ComputeJobDataHolder>> jobFut;
            if (mapper != null) {
                jobFut = requiredTable(tableName)
                        .thenCompose(table -> primaryReplicaForPartitionByMappedKey(table, key, mapper)
                                .thenApply(primaryNode -> executeOnOneNodeWithFailover(
                                        primaryNode,
                                        new NextColocatedWorkerSelector<>(placementDriver, topologyService, clock, table, key, mapper),
                                        descriptor.units(),
                                        descriptor.jobClassName(),
                                        descriptor.options(),
                                        argHolder,
                                        cancellationToken
                                )));

            } else {
                jobFut = requiredTable(tableName)
                        .thenCompose(table -> submitColocatedInternal(
                                table,
                                (Tuple) key,
                                descriptor.units(),
                                descriptor.jobClassName(),
                                descriptor.options(),
                                argHolder,
                                cancellationToken
                        ));
            }

            return jobFut.thenApply(execution -> unmarshalResult(execution, descriptor));
        }

        throw new IllegalArgumentException("Unsupported job target: " + target);
    }

    @Override
    public <T, R> CompletableFuture<BroadcastExecution<R>> submitAsync(
            BroadcastJobTarget target,
            JobDescriptor<T, R> descriptor,
            @Nullable T arg,
            @Nullable CancellationToken cancellationToken
    ) {
        Objects.requireNonNull(target);
        Objects.requireNonNull(descriptor);

        ComputeJobDataHolder argHolder = SharedComputeUtils.marshalArgOrResult(arg, descriptor.argumentMarshaller());

        if (target instanceof AllNodesBroadcastJobTarget) {
            AllNodesBroadcastJobTarget allNodesBroadcastTarget = (AllNodesBroadcastJobTarget) target;
            Set<ClusterNode> nodes = allNodesBroadcastTarget.nodes();

            return completedFuture(new BroadcastJobExecutionImpl<>(nodes.stream()
                    .map(node -> submitForBroadcast(node, descriptor, argHolder, cancellationToken))
                    .collect(toList()))
            );
        } else if (target instanceof TableJobTarget) {
            TableJobTarget tableJobTarget = (TableJobTarget) target;
            return requiredTable(tableJobTarget.tableName())
                    .thenCompose(table -> table.partitionManager().primaryReplicasAsync())
                    .thenApply(replicas -> new BroadcastJobExecutionImpl<>(
                            replicas.entrySet().stream()
                                    .map(entry -> submitForBroadcast(
                                            entry.getValue(),
                                            entry.getKey(),
                                            descriptor,
                                            argHolder,
                                            cancellationToken
                                    ))
                                    .collect(toList())
                    ));
        }

        throw new IllegalArgumentException("Unsupported job target: " + target);
    }

    private <T, R> JobExecution<R> submitForBroadcast(
            ClusterNode node,
            JobDescriptor<T, R> descriptor,
            @Nullable ComputeJobDataHolder argHolder,
            @Nullable CancellationToken cancellationToken
    ) {
        ExecutionOptions options = ExecutionOptions.from(descriptor.options());
        return submitForBroadcast(node, descriptor, options, argHolder, cancellationToken);
    }

    private <T, R> JobExecution<R> submitForBroadcast(
            ClusterNode node,
            @Nullable Partition partition,
            JobDescriptor<T, R> descriptor,
            @Nullable ComputeJobDataHolder argHolder,
            @Nullable CancellationToken cancellationToken
    ) {
        ExecutionOptions options = ExecutionOptions.builder()
                .priority(descriptor.options().priority())
                .maxRetries(descriptor.options().maxRetries())
                .partition(partition)
                .build();

        // TODO https://issues.apache.org/jira/browse/IGNITE-24027
        return submitForBroadcast(node, descriptor, options, argHolder, cancellationToken);
    }

    private <T, R> JobExecution<R> submitForBroadcast(
            ClusterNode node,
            JobDescriptor<T, R> descriptor,
            ExecutionOptions options,
            @Nullable ComputeJobDataHolder argHolder,
            @Nullable CancellationToken cancellationToken
    ) {
        if (topologyService.getByConsistentId(node.name()) == null) {
            return new FailedExecution<>(new NodeNotFoundException(Set.of(node.name())));
        }

        // No failover nodes for broadcast. We use failover here in order to complete futures with exceptions
        // if worker node has left the cluster.
        return unmarshalResult(
                executeOnOneNodeWithFailover(
                        node,
                        CompletableFutures::nullCompletedFuture,
                        descriptor.units(),
                        descriptor.jobClassName(),
                        options,
                        argHolder,
                        cancellationToken
                ),
                descriptor
        );
    }

    private static <T, R> JobExecution<R> unmarshalResult(JobExecution<ComputeJobDataHolder> execution, JobDescriptor<T, R> descriptor) {
        return new ResultUnmarshallingJobExecution<>(
                execution,
                descriptor.resultMarshaller(),
                descriptor.resultClass()
        );
    }

    @Override
    public <T, R> R execute(
            JobTarget target,
            JobDescriptor<T, R> descriptor,
            @Nullable T arg,
            @Nullable CancellationToken cancellationToken
    ) {
        return sync(executeAsync(target, descriptor, arg, cancellationToken));
    }

    @Override
    public <T, R> Collection<R> execute(
            BroadcastJobTarget target,
            JobDescriptor<T, R> descriptor,
            @Nullable T arg,
            @Nullable CancellationToken cancellationToken
    ) {
        return sync(executeAsync(target, descriptor, arg, cancellationToken));
    }

    @Override
    public JobExecution<ComputeJobDataHolder> executeAsyncWithFailover(
            Set<ClusterNode> nodes,
            List<DeploymentUnit> units,
            String jobClassName,
            JobExecutionOptions options,
            @Nullable ComputeJobDataHolder arg,
            @Nullable CancellationToken cancellationToken
    ) {
        Set<ClusterNode> candidates = new HashSet<>();
        for (ClusterNode node : nodes) {
            if (topologyService.getByConsistentId(node.name()) != null) {
                candidates.add(node);
            }
        }
        if (candidates.isEmpty()) {
            Set<String> nodeNames = nodes.stream().map(ClusterNode::name).collect(Collectors.toSet());
            return new FailedExecution<>(new NodeNotFoundException(nodeNames));
        }

        ClusterNode targetNode = randomNode(candidates);
        candidates.remove(targetNode);

        NextWorkerSelector selector = new DeqNextWorkerSelector(new ConcurrentLinkedDeque<>(candidates));

        return executeOnOneNodeWithFailover(
                targetNode,
                selector,
                units,
                jobClassName,
                options,
                arg,
                cancellationToken
        );
    }

    private static ClusterNode randomNode(Set<ClusterNode> nodes) {
        int nodesToSkip = ThreadLocalRandom.current().nextInt(nodes.size());

        Iterator<ClusterNode> iterator = nodes.iterator();
        for (int i = 0; i < nodesToSkip; i++) {
            iterator.next();
        }

        return iterator.next();
    }

    // TODO https://issues.apache.org/jira/browse/IGNITE-24184
    private JobExecution<ComputeJobDataHolder> executeOnOneNodeWithFailover(
            ClusterNode targetNode,
            NextWorkerSelector nextWorkerSelector,
            List<DeploymentUnit> units,
            String jobClassName,
            JobExecutionOptions jobExecutionOptions,
            @Nullable ComputeJobDataHolder arg,
            @Nullable CancellationToken cancellationToken
    ) {
        ExecutionOptions options = ExecutionOptions.from(jobExecutionOptions);
        return executeOnOneNodeWithFailover(targetNode, nextWorkerSelector, units, jobClassName, options, arg, cancellationToken);
    }

    private JobExecution<ComputeJobDataHolder> executeOnOneNodeWithFailover(
            ClusterNode targetNode,
            NextWorkerSelector nextWorkerSelector,
            List<DeploymentUnit> units,
            String jobClassName,
            ExecutionOptions options,
            @Nullable ComputeJobDataHolder arg,
            @Nullable CancellationToken cancellationToken
    ) {
        if (isLocal(targetNode)) {
            return new JobExecutionWrapper<>(computeComponent.executeLocally(options, units, jobClassName, arg, cancellationToken));
        } else {
            return new JobExecutionWrapper<>(computeComponent.executeRemotelyWithFailover(
                    targetNode, nextWorkerSelector, units, jobClassName, options, arg, cancellationToken));
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
        return targetNode.name().equals(topologyService.localMember().name());
    }

    @Override
    public CompletableFuture<JobExecution<ComputeJobDataHolder>> submitColocatedInternal(
            TableViewInternal table,
            Tuple key,
            List<DeploymentUnit> units,
            String jobClassName,
            JobExecutionOptions options,
            @Nullable ComputeJobDataHolder arg,
            @Nullable CancellationToken cancellationToken
    ) {
        return primaryReplicaForPartitionByTupleKey(table, key)
                .thenApply(primaryNode -> executeOnOneNodeWithFailover(
                        primaryNode,
                        new NextColocatedWorkerSelector<>(placementDriver, topologyService, clock, table, key),
                        units, jobClassName, options, arg, cancellationToken
                ));
    }

    @Override
    public CompletableFuture<JobExecution<ComputeJobDataHolder>> submitPartitionedInternal(
            TableViewInternal table,
            int partitionId,
            List<DeploymentUnit> units,
            String jobClassName,
            JobExecutionOptions jobExecutionOptions,
            @Nullable ComputeJobDataHolder arg,
            @Nullable CancellationToken cancellationToken
    ) {
        ExecutionOptions options = ExecutionOptions.builder()
                .priority(jobExecutionOptions.priority())
                .maxRetries(jobExecutionOptions.maxRetries())
                .partition(new HashPartition(partitionId))
                .build();

        return primaryReplicaForPartition(table, partitionId)
                .thenApply(primaryNode -> executeOnOneNodeWithFailover(
                        primaryNode,
                        // TODO https://issues.apache.org/jira/browse/IGNITE-24027
                        CompletableFutures::nullCompletedFuture,
                        units, jobClassName, options, arg, cancellationToken
                ));
    }

    private CompletableFuture<TableViewInternal> requiredTable(String tableName) {
        String parsedName = IgniteNameUtils.parseSimpleName(tableName);

        return tables.tableViewAsync(parsedName)
                .thenApply(table -> {
                    if (table == null) {
                        throw new TableNotFoundException(SqlCommon.DEFAULT_SCHEMA_NAME, parsedName);
                    }
                    return table;
                });
    }

    private CompletableFuture<ClusterNode> primaryReplicaForPartitionByTupleKey(TableViewInternal table, Tuple key) {
        return primaryReplicaForPartition(table, table.partitionId(key));
    }

    private <K> CompletableFuture<ClusterNode> primaryReplicaForPartitionByMappedKey(TableViewInternal table, K key,
            Mapper<K> keyMapper) {
        return primaryReplicaForPartition(table, table.partitionId(key, keyMapper));
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

    @Override
    public <T, R> TaskExecution<R> submitMapReduce(
            TaskDescriptor<T, R> taskDescriptor,
            @Nullable T arg,
            @Nullable CancellationToken cancellationToken
    ) {
        Objects.requireNonNull(taskDescriptor);

        CancellableTaskExecution<R> taskExecution = computeComponent.executeTask(
                this::submitJobs,
                taskDescriptor.units(),
                taskDescriptor.taskClassName(),
                arg
        );

        if (cancellationToken != null) {
            CancelHandleHelper.addCancelAction(cancellationToken, taskExecution::cancelAsync, taskExecution.resultAsync());
        }

        return new TaskExecutionWrapper<>(taskExecution);
    }

    @Override
    public <T, R> R executeMapReduce(
            TaskDescriptor<T, R> taskDescriptor,
            @Nullable T arg,
            @Nullable CancellationToken cancellationToken
    ) {
        return sync(executeMapReduceAsync(taskDescriptor, arg, cancellationToken));
    }

    private <M, T> CompletableFuture<List<JobExecution<T>>> submitJobs(
            List<MapReduceJob<M, T>> runners,
            CancellationToken cancellationToken
    ) {
        return allOfToList(
                runners.stream()
                        .map(runner -> submitAsync(
                                JobTarget.anyNode(runner.nodes()),
                                runner.jobDescriptor(),
                                runner.arg(),
                                cancellationToken
                        ))
                        .toArray(CompletableFuture[]::new)
        );
    }

    @Override
    public CompletableFuture<Collection<JobState>> statesAsync() {
        return computeComponent.statesAsync();
    }

    @Override
    public CompletableFuture<@Nullable JobState> stateAsync(UUID jobId) {
        return computeComponent.stateAsync(jobId);
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
    public <A, I, R> CompletableFuture<Collection<R>> runReceiverAsync(
            ReceiverDescriptor<A> receiver,
            @Nullable A receiverArg,
            Collection<I> items,
            ClusterNode node,
            List<DeploymentUnit> deploymentUnits) {
        var payload = StreamerReceiverSerializer.serializeReceiverInfoWithElementCount(receiver, receiverArg, items);

        return runReceiverAsync(payload, node, deploymentUnits)
                .thenApply(StreamerReceiverSerializer::deserializeReceiverJobResults);
    }

    @Override
    public CompletableFuture<byte[]> runReceiverAsync(byte[] payload, ClusterNode node, List<DeploymentUnit> deploymentUnits) {
        // Use Compute to execute receiver on the target node with failover, class loading, scheduling.
        JobExecution<ComputeJobDataHolder> jobExecution = executeAsyncWithFailover(
                Set.of(node),
                deploymentUnits,
                StreamerReceiverJob.class.getName(),
                JobExecutionOptions.DEFAULT,
                SharedComputeUtils.marshalArgOrResult(payload, null),
                null
        );

        return jobExecution.resultAsync()
                .handle((res, err) -> {
                    if (err != null) {
                        if (err.getCause() instanceof ComputeException) {
                            ComputeException computeErr = (ComputeException) err.getCause();
                            throw new IgniteException(
                                    COMPUTE_JOB_FAILED_ERR,
                                    "Streamer receiver failed: " + computeErr.getMessage(), computeErr);
                        }

                        ExceptionUtils.sneakyThrow(err);
                    }

                    return SharedComputeUtils.unmarshalArgOrResult(res, null, null);
                });
    }

    /** Returns a {@link OperationKillHandler kill handler} for the compute job. */
    public OperationKillHandler killHandler() {
        return new OperationKillHandler() {
            @Override
            public CompletableFuture<Boolean> cancelAsync(String operationId) {
                UUID jobId = UUID.fromString(operationId);

                return IgniteComputeImpl.this.cancelAsync(jobId)
                        .thenApply(res -> res != null ? res : Boolean.FALSE);
            }

            @Override
            public boolean local() {
                return false;
            }

            @Override
            public CancellableOperationType type() {
                return CancellableOperationType.COMPUTE;
            }
        };
    }

    @TestOnly
    ComputeComponent computeComponent() {
        return computeComponent;
    }

    private static <R> R sync(CompletableFuture<R> future) {
        try {
            return future.join();
        } catch (CompletionException e) {
            throw ExceptionUtils.sneakyThrow(mapToPublicException(unwrapCause(e)));
        }
    }
}
