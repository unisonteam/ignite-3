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
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.compute.DeploymentUnit;
import org.apache.ignite.compute.IgniteCompute;
import org.apache.ignite.compute.JobExecution;
import org.apache.ignite.compute.JobExecutionOptions;
import org.jetbrains.annotations.Nullable;

public class SplitTaskImpl implements SplitTask {
    private final String jobClassName;

    private final JobExecutionOptions jobOptions;

    private final List<DeploymentUnit> units;

    @Nullable
    private final Object[] args;

    private SplitTaskImpl(
            String jobClassName,
            JobExecutionOptions jobOptions,
            List<DeploymentUnit> units,
            Object[] args
    ) {
        this.jobClassName = jobClassName;
        this.jobOptions = jobOptions;
        this.units = units;
        this.args = args;
    }

    @Override
    public JobExecution<Object> execute(IgniteCompute compute) {
        throw new UnsupportedOperationException();
    }

    public SplitTaskBuilder toBuilder() {
        return builder().jobClassName(jobClassName).options(jobOptions).units(units).args(args);
    }

    public static SplitTaskBuilder builder() {
        return new SplitTaskBuilder();
    }

    public static class SplitTaskBuilder {
        private String jobClassName;

        private JobExecutionOptions jobOptions = JobExecutionOptions.DEFAULT;

        private List<DeploymentUnit> units = Collections.emptyList();

        @Nullable
        private Object[] args;

        public SplitTaskBuilder jobClassName(String jobClassName) {
            this.jobClassName = jobClassName;
            return this;
        }

        public SplitTaskBuilder options(JobExecutionOptions options) {
            this.jobOptions = options;
            return this;
        }

        public SplitTaskBuilder units(List<DeploymentUnit> units) {
            this.units = units;
            return this;
        }

        public SplitTaskBuilder args(Object[] args) {
            this.args = args;
            return this;
        }

        public SplitTask build() {
            return new SplitTaskImpl(
                    jobClassName,
                    jobOptions,
                    units,
                    args
            );
        }
    }
}
