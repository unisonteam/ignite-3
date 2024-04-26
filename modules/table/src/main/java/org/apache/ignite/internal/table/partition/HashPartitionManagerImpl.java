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

package org.apache.ignite.internal.table.partition;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.IntStream;
import org.apache.ignite.internal.lang.IgniteInternalException;
import org.apache.ignite.internal.marshaller.MarshallerException;
import org.apache.ignite.internal.marshaller.MarshallersProvider;
import org.apache.ignite.internal.replicator.TablePartitionId;
import org.apache.ignite.internal.schema.BinaryRowEx;
import org.apache.ignite.internal.schema.SchemaRegistry;
import org.apache.ignite.internal.schema.marshaller.TupleMarshallerException;
import org.apache.ignite.internal.schema.marshaller.TupleMarshallerImpl;
import org.apache.ignite.internal.schema.marshaller.reflection.KvMarshallerImpl;
import org.apache.ignite.internal.schema.row.Row;
import org.apache.ignite.internal.table.InternalTable;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.table.Tuple;
import org.apache.ignite.table.mapper.Mapper;
import org.apache.ignite.table.partition.HashPartition;
import org.apache.ignite.table.partition.PartitionManager;

/**
 * Implementation of {@link PartitionManager} for tables with hash partitions.
 */
public class HashPartitionManagerImpl implements PartitionManager<HashPartition> {
    private final InternalTable table;

    private final SchemaRegistry schemaReg;

    private final MarshallersProvider marshallers;

    /**
     * Constructor.
     *
     * @param table Internal table.
     * @param schemaReg Schema registry.
     * @param marshallers Marshallers.
     */
    public HashPartitionManagerImpl(
            InternalTable table,
            SchemaRegistry schemaReg,
            MarshallersProvider marshallers
    ) {
        this.table = table;
        this.schemaReg = schemaReg;
        this.marshallers = marshallers;
    }

    @Override
    public CompletableFuture<ClusterNode> partitionLocationAsync(HashPartition partition) {
        return table.partitionLocation(new TablePartitionId(table.tableId(), partition.partitionId()));
    }

    @Override
    public CompletableFuture<Map<HashPartition, ClusterNode>> allPartitionsAsync() {
        Map<HashPartition, ClusterNode> resultMap = new ConcurrentHashMap<>();

        HashPartition[] allPartitions = IntStream.range(0, table.partitions())
                .mapToObj(HashPartition::new)
                .toArray(HashPartition[]::new);

        CompletableFuture<?>[] futures = new CompletableFuture<?>[allPartitions.length];
        for (int i = 0; i < allPartitions.length; i++) {
            HashPartition partition = allPartitions[i];
            futures[i] = table.partitionLocation(new TablePartitionId(table.tableId(), partition.partitionId()))
                    .thenAccept(node -> resultMap.put(partition, node));
        }

        return CompletableFuture.allOf(futures)
                .thenApply(unused -> resultMap);
    }

    @Override
    public <K> CompletableFuture<HashPartition> partitionFromKeyAsync(K key, Mapper<K> mapper) {
        Objects.requireNonNull(key);
        Objects.requireNonNull(mapper);

        BinaryRowEx keyRow;
        var marshaller = new KvMarshallerImpl<>(schemaReg.lastKnownSchema(), marshallers, mapper, mapper);
        try {
            keyRow = marshaller.marshal(key);
        } catch (MarshallerException e) {
            throw new IgniteInternalException("Cannot marshal key", e);
        }

        return CompletableFuture.completedFuture(new HashPartition(table.partition(keyRow)));
    }

    @Override
    public CompletableFuture<HashPartition> partitionFromKeyAsync(Tuple key) {
        Objects.requireNonNull(key);

        try {
            // Taking latest schema version for marshaller here because it's only used to calculate colocation hash, and colocation
            // columns never change (so they are the same for all schema versions of the table),
            Row keyRow = new TupleMarshallerImpl(schemaReg.lastKnownSchema()).marshalKey(key);

            return CompletableFuture.completedFuture(new HashPartition(table.partition(keyRow)));
        } catch (TupleMarshallerException e) {
            throw new IgniteInternalException(e);
        }
    }
}
