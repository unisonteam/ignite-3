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

package org.apache.ignite.internal.cli.commands.cluster.topology;

import static org.apache.ignite.internal.cli.commands.CommandConstants.ABBREVIATE_SYNOPSIS;
import static org.apache.ignite.internal.cli.commands.CommandConstants.COMMAND_LIST_HEADING;
import static org.apache.ignite.internal.cli.commands.CommandConstants.DESCRIPTION_HEADING;
import static org.apache.ignite.internal.cli.commands.CommandConstants.OPTION_LIST_HEADING;
import static org.apache.ignite.internal.cli.commands.CommandConstants.PARAMETER_LIST_HEADING;
import static org.apache.ignite.internal.cli.commands.CommandConstants.REQUIRED_OPTION_MARKER;
import static org.apache.ignite.internal.cli.commands.CommandConstants.SORT_OPTIONS;
import static org.apache.ignite.internal.cli.commands.CommandConstants.SORT_SYNOPSIS;
import static org.apache.ignite.internal.cli.commands.CommandConstants.SYNOPSIS_HEADING;
import static org.apache.ignite.internal.cli.commands.CommandConstants.USAGE_HELP_AUTO_WIDTH;
import static org.apache.ignite.internal.cli.commands.Options.Constants.PLAIN_OPTION;
import static org.apache.ignite.internal.cli.commands.Options.Constants.PLAIN_OPTION_DESC;

import jakarta.inject.Inject;
import java.util.concurrent.Callable;
import org.apache.ignite.internal.cli.call.cluster.topology.LogicalTopologyCall;
import org.apache.ignite.internal.cli.commands.BaseCommand;
import org.apache.ignite.internal.cli.commands.cluster.ClusterUrlProfileMixin;
import org.apache.ignite.internal.cli.core.call.CallExecutionPipeline;
import org.apache.ignite.internal.cli.core.call.UrlCallInput;
import org.apache.ignite.internal.cli.core.exception.handler.ClusterNotInitializedExceptionHandler;
import org.apache.ignite.internal.cli.decorators.TopologyDecorator;
import picocli.CommandLine.Command;
import picocli.CommandLine.Mixin;
import picocli.CommandLine.Option;

/**
 * Command that show logical cluster topology.
 */
@Command(
        name = "logical",

        descriptionHeading = DESCRIPTION_HEADING,
        optionListHeading = OPTION_LIST_HEADING,
        synopsisHeading = SYNOPSIS_HEADING,
        requiredOptionMarker = REQUIRED_OPTION_MARKER,
        usageHelpAutoWidth = USAGE_HELP_AUTO_WIDTH,
        sortOptions = SORT_OPTIONS,
        sortSynopsis = SORT_SYNOPSIS,
        abbreviateSynopsis = ABBREVIATE_SYNOPSIS,
        commandListHeading = COMMAND_LIST_HEADING,
        parameterListHeading = PARAMETER_LIST_HEADING
)
public class LogicalTopologyCommand extends BaseCommand implements Callable<Integer> {
    /** Cluster endpoint URL option. */
    @Mixin
    private ClusterUrlProfileMixin clusterUrl;

    @Inject
    private LogicalTopologyCall call;

    @Option(names = PLAIN_OPTION, description = PLAIN_OPTION_DESC)
    private boolean plain;

    /** {@inheritDoc} */
    @Override
    public Integer call() {
        return CallExecutionPipeline.builder(call)
                .inputProvider(() -> new UrlCallInput(clusterUrl.getClusterUrl()))
                .output(spec.commandLine().getOut())
                .errOutput(spec.commandLine().getErr())
                .decorator(new TopologyDecorator(plain))
                .exceptionHandler(ClusterNotInitializedExceptionHandler.createHandler("Cannot show logical topology"))
                .verbose(verbose)
                .build()
                .runPipeline();
    }
}
