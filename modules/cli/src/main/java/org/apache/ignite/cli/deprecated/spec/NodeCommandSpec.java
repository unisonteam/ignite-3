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

package org.apache.ignite.cli.deprecated.spec;

import com.jakewharton.fliptables.FlipTable;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.Callable;
import org.apache.ignite.cli.commands.BaseCommand;
import org.apache.ignite.cli.core.style.SuccessElement;
import org.apache.ignite.cli.deprecated.CliPathsConfigLoader;
import org.apache.ignite.cli.deprecated.IgniteCliException;
import org.apache.ignite.cli.deprecated.IgnitePaths;
import org.apache.ignite.cli.deprecated.builtins.node.NodeManager;
import picocli.CommandLine;
import picocli.CommandLine.Help.Ansi;
import picocli.CommandLine.Help.ColorScheme;

/**
 * Commands for start/stop/list Ignite nodes on the current machine.
 */
@CommandLine.Command(
        name = "node",
        description = "Manages locally running Ignite nodes",
        subcommands = {
                NodeCommandSpec.StartNodeCommandSpec.class,
                NodeCommandSpec.StopNodeCommandSpec.class,
                NodeCommandSpec.NodesClasspathCommandSpec.class,
                NodeCommandSpec.ListNodesCommandSpec.class
        }
)
public class NodeCommandSpec {
    /**
     * Starts Ignite node command.
     */
    @CommandLine.Command(name = "start", description = "Starts an Ignite node locally")
    @Singleton
    public static class StartNodeCommandSpec extends BaseCommand implements Callable<Integer> {

        /** Loader for Ignite distributive paths. */
        @Inject
        private CliPathsConfigLoader cliPathsCfgLdr;

        /** Node manager. */
        @Inject
        private NodeManager nodeMgr;

        /** Consistent id, which will be used by new node. */
        @CommandLine.Parameters(paramLabel = "name", description = "Name of the new node")
        public String nodeName;

        /** Path to node config. */
        @CommandLine.Option(names = "--config", description = "Configuration file to start the node with")
        private Path configPath;

        /** {@inheritDoc} */
        @Override
        public Integer call() {
            IgnitePaths ignitePaths = cliPathsCfgLdr.loadIgnitePathsOrThrowError();

            PrintWriter out = spec.commandLine().getOut();
            ColorScheme cs = spec.commandLine().getColorScheme();

            NodeManager.RunningNode node = nodeMgr.start(
                    nodeName,
                    ignitePaths.nodesBaseWorkDir(),
                    ignitePaths.logDir,
                    ignitePaths.cliPidsDir(),
                    configPath,
                    ignitePaths.serverJavaUtilLoggingPros(),
                    out);

            out.println(SuccessElement.done().render());

            out.println(String.format("[name: %s, pid: %d]", node.name, node.pid));

            out.println();
            out.println("Node is successfully started. To stop, type "
                    + cs.commandText("ignite node stop ") + cs.parameterText(node.name));

            return 0;
        }
    }

    /**
     * Command for stopping Ignite node on the current machine.
     */
    @CommandLine.Command(name = "stop", description = "Stops a locally running Ignite node.")
    public static class StopNodeCommandSpec extends BaseCommand implements Callable<Integer> {
        /** Node manager. */
        @Inject
        private NodeManager nodeMgr;

        /** Loader for Ignite distributive paths. */
        @Inject
        private CliPathsConfigLoader cliPathsCfgLdr;

        /** Consistent ids of nodes to stop. */
        @CommandLine.Parameters(
                arity = "1..*",
                paramLabel = "consistent-ids",
                description = "Consistent IDs of the nodes to stop (space separated list)"
        )
        private List<String> consistentIds;

        /** {@inheritDoc} */
        @Override
        public Integer call() {
            IgnitePaths ignitePaths = cliPathsCfgLdr.loadIgnitePathsOrThrowError();

            PrintWriter out = spec.commandLine().getOut();
            ColorScheme cs = spec.commandLine().getColorScheme();

            consistentIds.forEach(p -> {
                out.println("Stopping locally running node with consistent ID " + cs.parameterText(p) + "...");

                if (nodeMgr.stopWait(p, ignitePaths.cliPidsDir())) {
                    out.println(cs.text("@|bold,green Done|@"));
                } else {
                    out.println(cs.text("@|bold,red Failed|@"));
                }
            });
            return 0;
        }
    }

    /**
     * Command for listing the running nodes.
     */
    @CommandLine.Command(name = "list", description = "Shows the list of currently running local Ignite nodes.")
    public static class ListNodesCommandSpec extends BaseCommand implements Callable<Integer> {
        /** Node manager. */
        @Inject
        private NodeManager nodeMgr;

        /** Loader for Ignite distributive paths. */
        @Inject
        private CliPathsConfigLoader cliPathsCfgLdr;

        /** {@inheritDoc} */
        @Override
        public Integer call() {
            IgnitePaths paths = cliPathsCfgLdr.loadIgnitePathsOrThrowError();

            List<NodeManager.RunningNode> nodes = nodeMgr.getRunningNodes(paths.logDir, paths.cliPidsDir());

            PrintWriter out = spec.commandLine().getOut();
            ColorScheme cs = spec.commandLine().getColorScheme();

            if (nodes.isEmpty()) {
                out.println("There are no locally running nodes");
                out.println("use the " + cs.commandText("ignite node start")
                        + " command to start a new node");
            } else {
                String[] headers = {"consistent id", "pid", "log file"};
                String[][] content = nodes.stream().map(
                        node -> new String[]{
                                node.name,
                                String.valueOf(node.pid),
                                String.valueOf(node.logFile)
                        }
                ).toArray(String[][]::new);

                out.println(FlipTable.of(headers, content));

                out.println("Number of running nodes: " + cs.text("@|bold " + nodes.size() + "|@"));
            }
            return 0;
        }
    }

    /**
     * Command for reading the current classpath of Ignite nodes.
     */
    @CommandLine.Command(name = "classpath", description = "Shows the current classpath used by the Ignite nodes.")
    public static class NodesClasspathCommandSpec extends BaseCommand implements Callable<Integer> {
        /** Node manager. */
        @Inject
        private NodeManager nodeMgr;

        /** {@inheritDoc} */
        @Override
        public Integer call() {
            try {
                List<String> items = nodeMgr.classpathItems();

                PrintWriter out = spec.commandLine().getOut();

                out.println(Ansi.AUTO.string("@|bold Current Ignite node classpath:|@"));

                for (String item : items) {
                    out.println("    " + item);
                }
            } catch (IOException e) {
                throw new IgniteCliException("Can't get current classpath", e);
            }
            return 0;
        }
    }
}
