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
import java.util.Objects;
import java.util.Set;
import org.apache.ignite.compute.JobExecutionOptions;
import org.apache.ignite.network.ClusterNode;
import org.jetbrains.annotations.Nullable;

public class TaskOptions {
    private final Set<ClusterNode> nodes;

    private final JobExecutionOptions options;

    @Nullable
    private final Object[] args;

    private TaskOptions(Set<ClusterNode> nodes, JobExecutionOptions options, Object[] args) {
        this.nodes = Collections.unmodifiableSet(nodes);
        this.options = options;
        this.args = args;
    }

    public Set<ClusterNode> nodes() {
        return nodes;
    }

    public JobExecutionOptions options() {
        return options;
    }

    @Nullable
    public Object[] args() {
        return args;
    }

    public static TaskArgsBuilder builder() {
        return new TaskArgsBuilder();
    }

    public static class TaskArgsBuilder {
        private Set<ClusterNode> nodes;

        private JobExecutionOptions options = JobExecutionOptions.DEFAULT;

        @Nullable
        private Object[] args;

        public TaskArgsBuilder nodes(Set<ClusterNode> nodes) {
            this.nodes = nodes;
            return this;
        }

        public TaskArgsBuilder options(JobExecutionOptions options) {
            this.options = options;
            return this;
        }

        public TaskArgsBuilder args(Object[] args) {
            this.args = args;
            return this;
        }

        public TaskOptions build() {
            Objects.requireNonNull(nodes);
            if (nodes.isEmpty()) {
                throw new IllegalArgumentException();
            }

            return new TaskOptions(nodes, options, args);
        }
    }
}
