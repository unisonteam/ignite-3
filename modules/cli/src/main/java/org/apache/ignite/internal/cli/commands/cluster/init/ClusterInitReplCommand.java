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
import static org.apache.ignite.internal.cli.commands.Options.Constants.HELP_OPTION;
import static org.apache.ignite.internal.cli.commands.Options.Constants.HELP_OPTION_DESC;
import static org.apache.ignite.internal.cli.commands.Options.Constants.HELP_OPTION_SHORT;
import static org.apache.ignite.internal.cli.commands.Options.Constants.VERBOSE_OPTION;
import static org.apache.ignite.internal.cli.commands.Options.Constants.VERBOSE_OPTION_DESC;
import static org.apache.ignite.internal.cli.commands.Options.Constants.VERBOSE_OPTION_SHORT;
import static picocli.CommandLine.Command;

import jakarta.inject.Inject;
import org.apache.ignite.internal.cli.call.cluster.ClusterInitCall;
import org.apache.ignite.internal.cli.call.cluster.ClusterInitCallInput;
import org.apache.ignite.internal.cli.commands.cluster.ClusterUrlMixin;
import org.apache.ignite.internal.cli.commands.questions.ConnectToClusterQuestion;
import org.apache.ignite.internal.cli.core.flow.builder.Flows;
import picocli.CommandLine.Mixin;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.Option;
import picocli.CommandLine.Spec;

/**
 * Initializes an Ignite cluster.
 */
@Command(
        name = "init",
        description = "Initializes an Ignite cluster",

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
public class ClusterInitReplCommand implements Runnable {
    @Mixin
    private ClusterInitOptions clusterInitOptions;

    /** Cluster endpoint URL option. */
    @Mixin
    private ClusterUrlMixin clusterUrl;

    /** Help option specification. */
    @Option(names = {HELP_OPTION, HELP_OPTION_SHORT}, usageHelp = true, description = HELP_OPTION_DESC)
    protected boolean usageHelpRequested;

    /** Verbose option specification. */
    @Option(names = {VERBOSE_OPTION, VERBOSE_OPTION_SHORT}, description = VERBOSE_OPTION_DESC)
    protected boolean verbose;

    /** Instance of picocli command specification. */
    @Spec
    protected CommandSpec spec;

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
