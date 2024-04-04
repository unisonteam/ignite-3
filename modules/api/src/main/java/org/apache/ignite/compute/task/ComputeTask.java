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

package org.apache.ignite.compute.task;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.BinaryOperator;
import java.util.stream.Collectors;
import org.apache.ignite.compute.JobExecutionContext;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.network.TopologyProvider;
import org.apache.ignite.table.partition.HashPartition;
import org.jetbrains.annotations.Nullable;

/**
 * Compute task interface.
 *
 * @param <R> Result type.
 */
public interface ComputeTask<R> {
    List<JobExecutionParameters> map(
            JobExecutionContext taskContext,
            PartitionProvider partitionProvider,
            @Nullable Object[] args
    );

    R reduce(Map<UUID, ?> results);


    static class Task implements ComputeTask<Integer> {
        @Override
        public List<JobExecutionParameters> map(JobExecutionContext taskContext, @Nullable Object[] args) {
            return taskContext.ignite()
                    .tables()
                    .table("tableName")
                    .partitions()
                    .allPartitions()
                    .entrySet()
                    .stream()
                    .map(e -> JobExecutionParameters.builder().node(e.getValue())
                            .jobClassName("className").args(e.getKey()).build()
                    ).collect(Collectors.toList());
        }

        @Override
        public Integer reduce(Map<UUID, ?> results) {
            return results.values().stream()
                    .map(Integer.class::cast)
                    .reduce(Integer::sum)
                    .orElse(0);
        }
    }
}
