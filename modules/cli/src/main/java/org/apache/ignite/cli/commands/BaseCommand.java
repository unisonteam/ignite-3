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

package org.apache.ignite.cli.commands;

import jakarta.inject.Inject;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;
import java.util.function.Supplier;
import org.apache.ignite.cli.call.connect.ConnectCall;
import org.apache.ignite.cli.call.connect.ConnectCallInput;
import org.apache.ignite.cli.config.Config;
import org.apache.ignite.cli.core.call.Call;
import org.apache.ignite.cli.core.call.CallInput;
import org.apache.ignite.cli.core.flow.Flowable;
import org.apache.ignite.cli.core.flow.builder.FlowBuilder;
import org.apache.ignite.cli.core.flow.builder.Flows;
import org.apache.ignite.cli.core.flow.question.QuestionAnswer;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.Option;
import picocli.CommandLine.Spec;

/**
 * Base class for commands.
 */
public abstract class BaseCommand {
    /** Help option specification. */
    @Option(names = {"-h", "--help"}, usageHelp = true, description = "Show this help message and exit.")
    protected boolean usageHelpRequested;

    @Spec
    protected CommandSpec spec;

    @Inject
    private ConnectCall connectCall;

    @Inject
    private Config config;


    protected <T extends CallInput, O> FlowBuilder<Void, O> callWithConnectQuestion(
            Supplier<String> clusterUrl,
            Function<String, T> clusterUrlMapper,
            Call<T, O> call) {
        String question = "You are not connected to node. Do you want to connect to the default node " + config.getProperty("ignite.cluster-url") + " ?";

        return Flows.from(clusterUrl.get())
                .ifThen(Objects::isNull, Flows.<String, ConnectCallInput>question(question,
                                List.of(
                                        new QuestionAnswer<>(BaseCommand::isAcceptedAnswer,
                                                s1 -> new ConnectCallInput(config.getProperty("ignite.cluster-url"))),
                                        new QuestionAnswer<>(s1 -> true, s1 -> Flowable.interrupt()))
                        ).appendFlow(Flows.fromCall(connectCall))
                        .build())
                .map(clusterUrlMapper)
                .appendFlow(Flows.fromCall(call))
                .output(spec.commandLine().getOut())
                .errorOutput(spec.commandLine().getErr());
    }

    private static boolean isAcceptedAnswer(String answer) {
        return answer.equalsIgnoreCase("y") || answer.isBlank();
    }
}
