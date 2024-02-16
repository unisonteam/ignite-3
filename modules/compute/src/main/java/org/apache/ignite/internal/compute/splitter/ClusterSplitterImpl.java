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

package org.apache.ignite.internal.compute.splitter;

import java.util.Collection;
import org.apache.ignite.compute.splitter.ClusterSplitter;
import org.apache.ignite.compute.splitter.Splitter;
import org.apache.ignite.compute.task.TableKey;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.network.TopologyProvider;
import org.apache.ignite.table.Tuple;

public class ClusterSplitterImpl implements ClusterSplitter {
    private final TopologyProvider topologyProvider;

    private final PartitionProvider partitionProvider;

    public ClusterSplitterImpl(TopologyProvider topologyProvider, PartitionProvider partitionProvider) {
        this.topologyProvider = topologyProvider;
        this.partitionProvider = partitionProvider;
    }

    @Override
    public Splitter<ClusterNode> forNodes() {
        return new NodesSplitter(topologyProvider);
    }

    @Override
    public Splitter<Integer> forRange(int range) {
        return null;
    }

    @Override
    public Splitter<Integer> forAllParts(String tableName) {
        return new PartitionSplitter(tableName, partitionProvider);
    }

    @Override
    public Splitter<Integer> forTuples(Collection<Tuple> tuples) {
        return null;
    }

    @Override
    public <T> Splitter<TableKey<T>> forKeys(Collection<TableKey<T>> tableKeys) {
        return null;
    }
}
