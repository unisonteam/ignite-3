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

import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.lang.IgniteInternalException;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.placementdriver.PlacementDriver;
import org.apache.ignite.internal.placementdriver.ReplicaMeta;
import org.apache.ignite.internal.replicator.TablePartitionId;
import org.apache.ignite.internal.table.IgniteTablesInternal;
import org.apache.ignite.internal.table.TableViewInternal;
import org.apache.ignite.lang.ErrorGroups.Common;
import org.apache.ignite.lang.TableNotFoundException;
import org.apache.ignite.lang.util.IgniteNameUtils;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.network.TopologyService;
import org.apache.ignite.table.Tuple;
import org.apache.ignite.table.mapper.Mapper;
import org.jetbrains.annotations.Nullable;

public class NextCollocatedWorkerSelector<K> implements NextWorkerSelector {
    private static final IgniteLogger LOG = Loggers.forClass(NextCollocatedWorkerSelector.class);

    private static final String DEFAULT_SCHEMA_NAME = "PUBLIC";

    private final IgniteTablesInternal tables;
    private final PlacementDriver placementDriver;
    private final TopologyService topologyService;
    private final String tableName;
    @Nullable
    private final K key;
    @Nullable
    private final Mapper<K> keyMapper;

    private final Tuple tuple;

    NextCollocatedWorkerSelector(IgniteTablesInternal tables, String tableName, Tuple tuple, PlacementDriver placementDriver,
            TopologyService topologyService) {
        this(tables, placementDriver, topologyService, tableName, null, null, tuple);
    }

    NextCollocatedWorkerSelector(IgniteTablesInternal tables, PlacementDriver placementDriver, TopologyService topologyService, String tableName, @Nullable K key,
            @Nullable Mapper<K> keyMapper, Tuple tuple) {
        this.tables = tables;
        this.placementDriver = placementDriver;
        this.topologyService = topologyService;
        this.tableName = tableName;
        this.key = key;
        this.keyMapper = keyMapper;
        this.tuple = tuple;
    }

    @Override
    public Optional<ClusterNode> next() {
        try {
            ReplicaMeta replicaMeta = requiredTable(tableName).thenApply(table -> {
                TablePartitionId tablePartitionId;
                if (key == null) {
                    tablePartitionId = new TablePartitionId(table.tableId(), table.partition(key, keyMapper));
                } else {
                    tablePartitionId = new TablePartitionId(table.tableId(), table.partition(tuple));
                }
                return placementDriver.getPrimaryReplica(tablePartitionId, HybridTimestamp.hybridTimestamp(0));
            }).get().get();
            return Optional.of(topologyService.getById(replicaMeta.getLeaseholderId()));
        } catch (InterruptedException | ExecutionException e) {
            LOG.warn("Failed to get primary replica for table " + tableName, e);
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

    private static <K> ClusterNode leaderOfTablePartitionByMappedKey(TableViewInternal table, K key, Mapper<K> keyMapper) {
        return requiredLeaderByPartition(table, table.partition(key, keyMapper));
    }

    private static ClusterNode requiredLeaderByPartition(TableViewInternal table, int partitionIndex) {
        ClusterNode leaderNode = table.leaderAssignment(partitionIndex);
        if (leaderNode == null) {
            throw new IgniteInternalException(Common.INTERNAL_ERR, "Leader not found for partition " + partitionIndex);
        }

        return leaderNode;
    }
}
