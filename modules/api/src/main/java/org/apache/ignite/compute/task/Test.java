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

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.ignite.compute.task.JobExecutionParameters.JobExecutionSplitter;
import org.apache.ignite.lang.Cursor;
import org.apache.ignite.network.ClusterNode;

public class Test {
    @FunctionalInterface
    interface Splitter<I, T> {
        List<JobExecutionParameters> split();
    }

    // Maps data of I type into T type
    @FunctionalInterface
    interface Mapper<I, T> {
        T map(I input);
    }

    // Reduces data from MapJobs from [T] into R
    @FunctionalInterface
    interface Reducer<T, R> {
        R reduce(Cursor<T> cursor);
    }

    // Task that "links" split job and reduce job.
    // I - input of MapJob that is returned by splitJob().split(tableName).values()
    // T - output of MapJob and input of ReduceJob
    // R - output of ReduceJob
    interface SplitReduceTask<I, T, R> extends Splitter<I, T>, Reducer<T, R> {

    }

    // MapReduce task that is a SplitReduceTask but with split by partition,
    // Also, defines the map task.
    interface MapReduceTask<I, T, R> extends SplitReduceTask<I, T, R>, Mapper<I, T> {
        @Override
        default List<JobExecutionParameters> split() {
            JobExecutionParameters.builder().jobClassName(this.getClass().getName())
            return new ComputePartitionSplit<>();
        }
    }

    static class ComputePartitionSplit<I, T> implements Splitter<I, T> {
        private final String tableName;
        private final JobExecutionParameters parameters;
        private final JobExecutionSplitter splitter = new JobExecutionSplitterImpl();

        ComputePartitionSplit(JobExecutionParameters parameters, String tableName) {
            this.parameters = parameters;
            this.tableName = tableName;
        }

        @Override
        public List<JobExecutionParameters> split() {
            return splitter.split(parameters, tableName);
        }
    }

    static class JobExecutionSplitterImpl implements JobExecutionSplitter {
        private static final Map<String, List<Integer>> tableToPartitionsMap = new HashMap<>();

        public static ClusterNode getPrimary(int partitions) {
            return null;
        }

        @Override
        public List<JobExecutionParameters> split(JobExecutionParameters parameters, String table) {
            return tableToPartitionsMap.get(table).stream()
                    .map(JobExecutionSplitterImpl::getPrimary)
                    .map(clusterNode -> parameters.toBuilder().nodes(Set.of(clusterNode)).build())
                    .collect(Collectors.toList());
        }
    }

    // Map job that defines the code that will consume
    // local data from partition primary replica.
    static class WordCountMap implements Mapper<String, Mapper<String, Integer>> {
        @Override
        public Map<String, Integer> map(Cursor<String> cursor) {
            HashMap<String, Integer> mapResult = new HashMap<>();


            cursor.forEachRemaining(text -> {
                String[] words = text.split(" ");
                Arrays.stream(words).forEach(word -> mapResult.merge(word, 1, Integer::sum));
            });


            return mapResult;
        }
    }

    // Reduce job that will be executed on "some" node and will
    // consume the outputs of Map jobs.
    static class WordCountReduce implements Reducer<Map<String, Integer>, Map<String, Integer>> {
        @Override
        public Map<String, Integer> reduce(Cursor<java.util.Map<String, Integer>> cursor) {
            HashMap<String, Integer> reduceResult = new HashMap<>();
            cursor.forEachRemaining(mapResult -> {
                mapResult.forEach((word, counts) -> {
                    reduceResult.merge(word, counts, Integer::sum);
                });
            });


            return reduceResult;
        }
    }

    // MapReduce task that "links" the WordCountMapJob and WordCountReduce job.
    // This is "type-safe".
    static class WordCountMapReduce implements MapReduceTask<String, java.util.Map<String, Integer>, java.util.Map<String, Integer>> {
        @Override
        public Mapper<String, Mapper<String, Integer>> mapJob() {
            return new WordCountMap();
        }


        @Override
        public Reducer<Map<String, Integer>, Map<String, Integer>> reduceJob() {
            return new WordCountReduce();
        }
    }
}
