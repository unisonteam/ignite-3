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

public class JobExecutionParameters {
    private final String jobClassName;

    private final Set<ClusterNode> nodes;

    private final JobExecutionOptions jobOptions;

    private final ColocationOption colocationOption;

    private final List<DeploymentUnit> units;

    @Nullable
    private final Object[] args;

    private JobExecutionParameters(
            String jobClassName,
            Set<ClusterNode> nodes,
            JobExecutionOptions jobOptions,
            ColocationOption colocationOption,
            List<DeploymentUnit> units,
            Object[] args
    ) {
        this.jobClassName = jobClassName;
        this.nodes = Collections.unmodifiableSet(nodes);
        this.jobOptions = jobOptions;
        this.colocationOption = colocationOption;
        this.units = units;
        this.args = args;
    }

    public String jobClassName() {
        return jobClassName;
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

    public JobExecutionParametersBuilder toBuilder() {
        return builder().jobClassName(jobClassName).nodes(nodes).options(jobOptions).units(units);
    }

    public static JobExecutionParametersBuilder builder() {
        return new JobExecutionParametersBuilder();
    }

    public static class JobExecutionParametersBuilder {
        private String jobClassName;

        private Set<ClusterNode> nodes;

        private ColocationOption colocationOptions;

        private JobExecutionOptions jobOptions = JobExecutionOptions.DEFAULT;

        private List<DeploymentUnit> units = Collections.emptyList();

        @Nullable
        private Object[] args;

        public JobExecutionParametersBuilder jobClassName(String jobClassName) {
            this.jobClassName = jobClassName;
            return this;
        }

        public JobExecutionParametersBuilder nodes(Set<ClusterNode> nodes) {
            this.nodes = nodes;
            return this;
        }

        public JobExecutionParametersBuilder options(JobExecutionOptions options) {
            this.jobOptions = options;
            return this;
        }

        public JobExecutionParametersBuilder units(List<DeploymentUnit> units) {
            this.units = units;
            return this;
        }

        public JobExecutionParametersBuilder colocatedWith(String tableName, Tuple tuple) {
            colocationOptions = new TupleColocationOption(tableName, tuple);
            return this;
        }

        public <T> JobExecutionParametersBuilder colocatedWith(String tableName, T key, Mapper<T> mapper) {
            colocationOptions = new KeyColocationOption<>(tableName, key, mapper);
            return this;
        }

        public JobExecutionParametersBuilder colocatedWith(String tableName) {
            colocationOptions = new TableColocationOption(tableName);
            return this;
        }

        public JobExecutionParametersBuilder args(Object[] args) {
            this.args = args;
            return this;
        }

        public JobExecutionParameters build() {
            Objects.requireNonNull(nodes);
            if (nodes.isEmpty()) {
                throw new IllegalArgumentException();
            }

            return new JobExecutionParameters(
                    jobClassName,
                    nodes,
                    jobOptions,
                    colocationOptions,
                    units,
                    args
            );
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

    private static class TableColocationOption extends ColocationOption {
        private TableColocationOption(String tableName) {
            super(tableName);
        }

        @Override
        public ClusterNode resolve() {
            return null;
        }
    }
}
