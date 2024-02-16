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
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.network.TopologyProvider;

public class NodesSplitter implements Splitter<ClusterNode> {
    private final TopologyProvider topologyProvider;

    public NodesSplitter(TopologyProvider topologyProvider) {
        this.topologyProvider = topologyProvider;
    }

    @Override
    public <R extends SplitTask, K extends Splitter<R> & Collector<R>> K split(Function<ClusterNode, R> map) {
        List<R> collect = topologyProvider.allMembers().stream().map(map).collect(Collectors.toList());
        return (K) new SplitterCollector<>(collect);
    }
}
