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

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import org.apache.ignite.compute.DeploymentUnit;
import org.apache.ignite.compute.JobExecutionOptions;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.table.Tuple;
import org.apache.ignite.table.mapper.Mapper;
import org.jetbrains.annotations.Nullable;

public class TaskOptions {
    private final Set<ClusterNode> nodes;

    private final JobExecutionOptions jobOptions;

    private final ColocationOption colocationOption;

    private final List<DeploymentUnit> units;

    @Nullable
    private final Object[] args;

    private TaskOptions(
            Set<ClusterNode> nodes,
            JobExecutionOptions jobOptions,
            ColocationOption colocationOption,
            List<DeploymentUnit> units,
            Object[] args
    ) {
        this.nodes = Collections.unmodifiableSet(nodes);
        this.jobOptions = jobOptions;
        this.colocationOption = colocationOption;
        this.units = units;
        this.args = args;
    }

    public Set<ClusterNode> nodes() {
        return nodes;
    }

    public JobExecutionOptions options() {
        return jobOptions;
    }

    public List<DeploymentUnit> units() {
        return units;
    }

    @Nullable
    public Object[] args() {
        return args;
    }

    public static TaskOptionsBuilder builder() {
        return new TaskOptionsBuilder();
    }

    public static class TaskOptionsBuilder {
        private Set<ClusterNode> nodes;

        private ColocationOption colocationOptions;

        private JobExecutionOptions jobOptions = JobExecutionOptions.DEFAULT;

        private List<DeploymentUnit> units = Collections.emptyList();

        @Nullable
        private Object[] args;

        public TaskOptionsBuilder nodes(Set<ClusterNode> nodes) {
            this.nodes = nodes;
            return this;
        }

        public TaskOptionsBuilder options(JobExecutionOptions options) {
            this.jobOptions = options;
            return this;
        }

        public TaskOptionsBuilder units(List<DeploymentUnit> units) {
            this.units = units;
            return this;
        }

        public TaskOptionsBuilder colocatedWith(String tableName, Tuple tuple) {
            colocationOptions = new TupleColocationOption(tableName, tuple);
            return this;
        }

        public <T> TaskOptionsBuilder colocatedWith(String tableName, T key, Mapper<T> mapper) {
            colocationOptions = new KeyColocationOption<>(tableName, key, mapper);
            return this;
        }

        public TaskOptionsBuilder args(Object[] args) {
            this.args = args;
            return this;
        }

        public TaskOptions build() {
            Objects.requireNonNull(nodes);
            if (nodes.isEmpty()) {
                throw new IllegalArgumentException();
            }

            return new TaskOptions(nodes, jobOptions, colocationOptions, units, args);
        }
    }

    private abstract static class ColocationOption {
        protected final String tableName;

        private ColocationOption(String tableName) {
            this.tableName = tableName;
        }

        public abstract ClusterNode resolve();
    }

    private static class TupleColocationOption extends ColocationOption {
        private final Tuple tuple;


        private TupleColocationOption(String tableName, Tuple tuple) {
            super(tableName);
            this.tuple = tuple;
        }

        @Override
        public ClusterNode resolve() {
            throw new UnsupportedOperationException();
        }
    }

    private static class KeyColocationOption<T> extends ColocationOption {
        private final T key;

        private final Mapper<T> mapper;


        private KeyColocationOption(String tableName, T key, Mapper<T> mapper) {
            super(tableName);
            this.key = key;
            this.mapper = mapper;
        }

        @Override
        public ClusterNode resolve() {
            throw new UnsupportedOperationException();
        }
    }
}
