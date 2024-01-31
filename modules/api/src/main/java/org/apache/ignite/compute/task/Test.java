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
import org.apache.ignite.compute.task.JobExecutionParameters.JobExecutionSplitter;
import org.apache.ignite.lang.Cursor;
import org.apache.ignite.network.ClusterNode;

public class Test {
    private static final Map<String, List<Integer>> tableToPartitionsMap = new HashMap<>();

    public static ClusterNode getPrimary(int partitions) {
        return null;
    }

    @FunctionalInterface
    interface SplitJob<I, T> {
        // [NodeName -> Map Job that should be executed in NodeName]
        List<JobExecutionParameters> split(String tableName);
    }


    // Maps data of I type into T type
    @FunctionalInterface
    interface MapJob<I, T> {
        T map(I input);
    }


    // Reduces data from MapJobs from [T] into R
    @FunctionalInterface
    interface ReduceJob<T, R> {
        R reduce(Cursor<T> cursor);
    }


    // Task that "links" split job and reduce job.
    // I - input of MapJob that is returned by splitJob().split(tableName).values()
    // T - output of MapJob and input of ReduceJob
    // R - output of ReduceJob
    interface SplitReduceTask<I, T, R> {
        SplitJob<I, T> splitJob();


        ReduceJob<T, R> reduceJob();
    }


    // MapReduce task that is a SplitReduceTask but with split by partition,
    // Also, defines the map task.
    interface MapReduceTask<I, T, R> extends SplitReduceTask<I, T, R> {
        @Override
        default SplitJob<I, T> splitJob() {
            return new PartitionSplitJob<>(this::mapJob);
        }


        MapJob<I, T> mapJob();
    }


    // In order not to use Class, because we'll have troubles with generics.
    @FunctionalInterface
    interface JobDefinition<J> {
        J newInstance();
    }


    // Split Job instance that gets the MapJobDefinition as an input
    // in constructor and the table name in the method argument.
    // It splits the MapJob in a way that each MapJob instance
    // Will be executed on PartitionPrimaryReplicas and only data from
    // local replica will be an input for local MapJob.
    static class PartitionSplitJob<I, T>  implements SplitJob<I, T> {
        private final JobDefinition<MapJob<I, T>> definition;
        private final String tableName;


        PartitionSplitJob(JobDefinition<MapJob<I, T>> definition, String tableName) {
            this.definition = definition;
            this.tableName = tableName;
        }


        @Override
        public List<JobExecutionParameters> split() {

            JobExecutionSplitter splitter = new JobExecutionSplitter() {

                @Override
                public List<JobExecutionParameters> split(JobExecutionParameters parameters, String table) {
                    return null;
                }
            };

            return splitter.split(JobExecutionParameters.builder().build(), tableName);
        }
    }


    // Map job that defines the code that will consume
    // local data from partition primary replica.
    static class WordCountMap implements MapJob<String, Map<String, Integer>> {
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
    static class WordCountReduce implements ReduceJob<Map<String, Integer>, Map<String, Integer>> {
        @Override
        public Map<String, Integer> reduce(Cursor<Map<String, Integer>> cursor) {
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
    static class WordCountMapReduce implements MapReduceTask<String, Map<String, Integer>, Map<String, Integer>> {
        @Override
        public MapJob<String, Map<String, Integer>> mapJob() {
            return new WordCountMap();
        }


        @Override
        public ReduceJob<Map<String, Integer>, Map<String, Integer>> reduceJob() {
            return new WordCountReduce();
        }


    }
}
