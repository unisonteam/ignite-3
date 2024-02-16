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

import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.ignite.compute.splitter.Collector;
import org.apache.ignite.compute.splitter.Splitter;
import org.apache.ignite.compute.task.SplitTask;

public class PartitionSplitter implements Splitter<Integer> {
    private final String tableName;

    private final PartitionProvider partitionProvider;

    public PartitionSplitter(String tableName, PartitionProvider partitionProvider) {
        this.tableName = tableName;
        this.partitionProvider = partitionProvider;
    }

    @Override
    public <R extends SplitTask, K extends Splitter<R> & Collector<R>> K split(Function<Integer, R> map) {
        List<AssignedSplitTask> collect = partitionProvider.partitions(tableName).entrySet().stream().map(e -> {
            R apply = map.apply(e.getKey());

            return new AssignedSplitTask(apply, e.getValue());
        }).collect(Collectors.toList());
        return (K) new SplitterCollector<>(collect);
    }
}
