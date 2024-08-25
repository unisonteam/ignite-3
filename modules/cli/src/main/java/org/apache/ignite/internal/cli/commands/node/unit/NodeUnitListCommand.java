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

package org.apache.ignite.internal.cli.commands.node.unit;


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
import org.apache.ignite.internal.cli.call.node.unit.NodeListUnitCall;
import org.apache.ignite.internal.cli.commands.BaseCommand;
import org.apache.ignite.internal.cli.commands.UnitListOptionsMixin;
import org.apache.ignite.internal.cli.commands.node.NodeUrlProfileMixin;
import org.apache.ignite.internal.cli.core.call.CallExecutionPipeline;
import org.apache.ignite.internal.cli.core.exception.handler.ClusterNotInitializedExceptionHandler;
import org.apache.ignite.internal.cli.decorators.UnitListDecorator;
import picocli.CommandLine.Command;
import picocli.CommandLine.Mixin;
import picocli.CommandLine.Option;

/** Command to list deployed units. */
@Command(
        name = "list",
        description = "Shows a list of deployed units",

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
public class NodeUnitListCommand extends BaseCommand implements Callable<Integer> {

    @Mixin
    private UnitListOptionsMixin listOptions;

    @Mixin
    private NodeUrlProfileMixin nodeUrl;

    @Option(names = PLAIN_OPTION, description = PLAIN_OPTION_DESC)
    private boolean plain;

    @Inject
    private NodeListUnitCall listUnitCall;

    @Override
    public Integer call() throws Exception {
        return CallExecutionPipeline.builder(listUnitCall)
                .inputProvider(() -> listOptions.toListUnitCallInput(nodeUrl.getNodeUrl()))
                .output(spec.commandLine().getOut())
                .errOutput(spec.commandLine().getErr())
                .verbose(verbose)
                .decorator(new UnitListDecorator(plain))
                .exceptionHandler(ClusterNotInitializedExceptionHandler.createHandler("Cannot list units"))
                .build().runPipeline();
    }
}
