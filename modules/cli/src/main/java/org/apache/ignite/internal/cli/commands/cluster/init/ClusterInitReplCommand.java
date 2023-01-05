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

package org.apache.ignite.internal.cli.commands.cluster.init;

import static picocli.CommandLine.Command;

import io.micronaut.core.annotation.ReflectiveAccess;
import jakarta.inject.Inject;
import org.apache.ignite.internal.cli.call.cluster.ClusterInitCall;
import org.apache.ignite.internal.cli.call.cluster.ClusterInitCallInput;
import org.apache.ignite.internal.cli.commands.BaseCommand;
import org.apache.ignite.internal.cli.commands.cluster.ClusterUrlMixin;
import org.apache.ignite.internal.cli.commands.questions.ConnectToClusterQuestion;
import org.apache.ignite.internal.cli.core.flow.builder.Flows;
import picocli.CommandLine.Mixin;

/**
 * Initializes an Ignite cluster.
 */
@Command(name = "init", description = "Initializes an Ignite cluster")
@ReflectiveAccess
public class ClusterInitReplCommand extends BaseCommand implements Runnable {
    /** Cluster endpoint URL option. */
    @Mixin
    private ClusterUrlMixin clusterUrl;

    @Mixin
    private ClusterInitOptions clusterInitOptions;

    @Inject
    private ClusterInitCall call;

    @Inject
    private ConnectToClusterQuestion question;

    /** {@inheritDoc} */
    @Override
    public void run() {
        question.askQuestionIfNotConnected(clusterUrl.getClusterUrl())
                .map(this::buildCallInput)
                .then(Flows.fromCall(call))
                .verbose(verbose)
                .print()
                .start();
    }

    private ClusterInitCallInput buildCallInput(String clusterUrl) {
        return ClusterInitCallInput.builder()
                .clusterUrl(clusterUrl)
                .fromClusterInitOptions(clusterInitOptions)
                .build();
    }
}
