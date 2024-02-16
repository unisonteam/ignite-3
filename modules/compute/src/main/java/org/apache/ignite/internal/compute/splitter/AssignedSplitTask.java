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

import org.apache.ignite.compute.IgniteCompute;
import org.apache.ignite.compute.JobExecution;
import org.apache.ignite.compute.task.SplitTask;
import org.apache.ignite.network.ClusterNode;

public class AssignedSplitTask implements SplitTask {
    private final SplitTask userTask;

    private final ClusterNode clusterNode;

    public AssignedSplitTask(SplitTask userTask, ClusterNode clusterNode) {
        this.userTask = userTask;
        this.clusterNode = clusterNode;
    }

    @Override
    public JobExecution<Object> execute(IgniteCompute compute) {
        //TODO: execute user task on clusterNode.
        throw new UnsupportedOperationException();
    }
}
