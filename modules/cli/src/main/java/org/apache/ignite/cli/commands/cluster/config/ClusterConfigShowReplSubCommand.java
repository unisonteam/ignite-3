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

package org.apache.ignite.cli.commands.cluster.config;

import jakarta.inject.Inject;
import org.apache.ignite.cli.call.configuration.ClusterConfigShowCall;
import org.apache.ignite.cli.call.configuration.ClusterConfigShowCallInput;
import org.apache.ignite.cli.commands.BaseCommand;
import org.apache.ignite.cli.commands.questions.ConnectToClusterQuestion;
import org.apache.ignite.cli.core.exception.ExceptionWriter;
import org.apache.ignite.cli.core.exception.IgniteCliApiException;
import org.apache.ignite.cli.core.exception.handler.IgniteCliApiExceptionHandler;
import org.apache.ignite.cli.core.flow.Flowable;
import org.apache.ignite.cli.core.flow.builder.Flows;
import org.apache.ignite.cli.core.style.AnsiStringSupport.Style;
import org.apache.ignite.cli.core.style.component.ErrorComponent;
import org.apache.ignite.rest.client.invoker.ApiException;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

/**
 * Command that shows configuration from the cluster in REPL mode.
 */
@Command(name = "show",
        description = "Shows cluster configuration")
public class ClusterConfigShowReplSubCommand extends BaseCommand implements Runnable {

    /**
     * Configuration selector option.
     */
    @Option(names = {"--selector"}, description = "Configuration path selector")
    private String selector;

    /**
     * Node url option.
     */
    @Option(names = {"--cluster-url"}, description = "Url to Ignite node")
    private String clusterUrl;

    @Inject
    private ClusterConfigShowCall call;

    @Inject
    private ConnectToClusterQuestion question;

    @Override
    public void run() {
        question.askQuestionIfNotConnected(clusterUrl)
                .map(this::configShowCallInput)
                .then(Flows.fromCall(call))
                .toOutput(spec.commandLine().getOut(), spec.commandLine().getErr())
                .exceptionHandler(new ShowConfigReplExceptionHandler())
                .build()
                .start(Flowable.empty());
    }

    private ClusterConfigShowCallInput configShowCallInput(String clusterUrl) {
        return ClusterConfigShowCallInput.builder().selector(selector).clusterUrl(clusterUrl).build();
    }

    private static class ShowConfigReplExceptionHandler extends IgniteCliApiExceptionHandler {
        @Override
        public int handle(ExceptionWriter err, IgniteCliApiException e) {
            if (e.getCause() instanceof ApiException) {
                ApiException apiException = (ApiException) e.getCause();
                if (apiException.getCode() == 500) { //TODO: should be 404
                    err.write(
                            ErrorComponent.builder()
                                    .header("Cannot show cluster config")
                                    .details("Probably you have not initialized the cluster, try to run " + Style.BOLD.mark("cluster init")
                                            + "command")
                                    .build()
                                    .render()
                    );
                    return 1;
                }
            }
            return super.handle(err, e);
        }
    }
}
