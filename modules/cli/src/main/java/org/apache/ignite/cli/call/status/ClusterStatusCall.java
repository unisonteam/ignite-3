/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.cli.call.status;

import jakarta.inject.Singleton;
import org.apache.ignite.cli.call.status.ClusterStatus.ClusterStatusBuilder;
import org.apache.ignite.cli.core.call.Call;
import org.apache.ignite.cli.core.call.CallOutput;
import org.apache.ignite.cli.core.call.DefaultCallOutput;
import org.apache.ignite.cli.core.call.StatusCallInput;
import org.apache.ignite.cli.core.exception.IgniteCliApiException;
import org.apache.ignite.cli.deprecated.CliPathsConfigLoader;
import org.apache.ignite.cli.deprecated.IgnitePaths;
import org.apache.ignite.cli.deprecated.builtins.node.NodeManager;
import org.apache.ignite.rest.client.api.ClusterManagementApi;
import org.apache.ignite.rest.client.invoker.ApiClient;
import org.apache.ignite.rest.client.invoker.ApiException;
import org.apache.ignite.rest.client.model.ClusterState;

/**
 * Call to get cluster status.
 */
@Singleton
public class ClusterStatusCall implements Call<StatusCallInput, ClusterStatus> {

    private final NodeManager nodeManager;

    private final CliPathsConfigLoader cliPathsCfgLdr;

    /**
     * Default constructor.
     */
    public ClusterStatusCall(NodeManager nodeManager, CliPathsConfigLoader cliPathsCfgLdr) {
        this.nodeManager = nodeManager;
        this.cliPathsCfgLdr = cliPathsCfgLdr;
    }

    @Override
    public CallOutput<ClusterStatus> execute(StatusCallInput input) {
        IgnitePaths paths = cliPathsCfgLdr.loadIgnitePathsOrThrowError();
        ClusterStatusBuilder clusterStatusBuilder = ClusterStatus.builder()
                .nodeCount(nodeManager.getRunningNodes(paths.logDir, paths.cliPidsDir()).size());

        try {
            ClusterState clusterState = fetchClusterState(input.getClusterUrl());
            clusterStatusBuilder
                    .initialized(true)
                    .name(clusterState.getClusterTag().getClusterName())
                    .cmgNodes(clusterState.getCmgNodes());
        } catch (ApiException e) {
            if (e.getCode() == 404) { // NOT_FOUND means the cluster is not initialized yet
                clusterStatusBuilder.initialized(false);
            } else {
                return DefaultCallOutput.failure(new IgniteCliApiException(e, input.getClusterUrl()));
            }
        } catch (IllegalArgumentException e) {
            clusterStatusBuilder.initialized(false);
        }

        return DefaultCallOutput.success(clusterStatusBuilder.build());
    }

    private ClusterState fetchClusterState(String url) throws ApiException {
        return new ClusterManagementApi(new ApiClient().setBasePath(url)).clusterState();
    }
}
