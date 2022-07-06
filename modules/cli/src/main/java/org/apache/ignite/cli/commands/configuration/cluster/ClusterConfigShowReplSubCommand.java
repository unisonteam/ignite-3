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

package org.apache.ignite.cli.commands.configuration.cluster;

import jakarta.inject.Inject;
import java.util.List;
import java.util.Objects;
import java.util.function.Predicate;
import org.apache.ignite.cli.call.configuration.ClusterConfigShowCall;
import org.apache.ignite.cli.call.configuration.ClusterConfigShowCallInput;
import org.apache.ignite.cli.call.configuration.JsonString;
import org.apache.ignite.cli.call.connect.ConnectCall;
import org.apache.ignite.cli.call.connect.ConnectCallInput;
import org.apache.ignite.cli.commands.BaseCommand;
import org.apache.ignite.cli.config.Config;
import org.apache.ignite.cli.core.call.StringCallInput;
import org.apache.ignite.cli.core.exception.ExceptionWriter;
import org.apache.ignite.cli.core.exception.IgniteCliApiException;
import org.apache.ignite.cli.core.exception.handler.IgniteCliApiExceptionHandler;
import org.apache.ignite.cli.core.flow.DefaultFlowable;
import org.apache.ignite.cli.core.flow.Flowable;
import org.apache.ignite.cli.core.flow.builder.Flow;
import org.apache.ignite.cli.core.flow.builder.FlowBuilder;
import org.apache.ignite.cli.core.flow.builder.Flows;
import org.apache.ignite.cli.core.flow.question.QuestionAnswer;
import org.apache.ignite.cli.core.flow.question.QuestionAsker;
import org.apache.ignite.cli.core.repl.Session;
import org.apache.ignite.rest.client.invoker.ApiException;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

/**
 * Command that shows configuration from the cluster in REPL mode.
 */
@Command(name = "show",
        description = "Shows cluster configuration.")
public class ClusterConfigShowReplSubCommand extends BaseCommand implements Runnable {

    /**
     * Configuration selector option.
     */
    @Option(names = {"--selector"}, description = "Configuration path selector.")
    private String selector;

    /**
     * Node url option.
     */
    @Option(
            names = {"--cluster-url"}, description = "Url to Ignite node."
    )
    private String clusterUrl;

    @Inject
    private ClusterConfigShowCall call;

    @Inject
    private Session session;

    @Inject
    private ConnectCall connectCall;

    @Inject
    private Config config;

    @Override
    public void run() {
        String question = "You are not connected to node. Do you want to connect to the default node?";

        Flows.from(getClusterUrl())
                .ifThen(Objects::isNull, Flows.<String, ConnectCallInput>question(question,
                        List.of(
                                new QuestionAnswer<>(s1 -> s1.equalsIgnoreCase("y"),
                                        s1 -> new ConnectCallInput(config.getProperty("ignite.cluster-url"))),
                                new QuestionAnswer<>(s1 -> true, s1 -> Flowable.interrupt()))
                        ).appendFlow(Flows.fromCall(connectCall))
                        .build())
                .map(clusterUrl1 -> ClusterConfigShowCallInput.builder().selector(selector).clusterUrl(getClusterUrl()).build())
                .appendFlow(Flows.fromCall(call))
                .exceptionHandler(new ShowConfigReplExceptionHandler())
                .output(spec.commandLine().getOut())
                .errorOutput(spec.commandLine().getErr())
                .build()
                .call(Flowable.empty());

    }

    private String getClusterUrl() {
        String s = null;
        if (session.isConnectedToNode()) {
            s = session.getNodeUrl();
        } else if (clusterUrl != null) {
            s = clusterUrl;
        }
        return s;
    }

    private static class ShowConfigReplExceptionHandler extends IgniteCliApiExceptionHandler {
        @Override
        public int handle(ExceptionWriter err, IgniteCliApiException e) {
            if (e.getCause() instanceof ApiException) {
                ApiException apiException = (ApiException) e.getCause();
                if (apiException.getCode() == 500) { //TODO: https://issues.apache.org/jira/browse/IGNITE-17091
                    err.write("Cannot show cluster config, probably you have not initialized the cluster. "
                            + "Try to run 'cluster init' command.");
                    return 1;
                }
            }
            return super.handle(err, e);
        }
    }
}
