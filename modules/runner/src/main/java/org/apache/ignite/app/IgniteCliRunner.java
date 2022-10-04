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

package org.apache.ignite.app;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigParseOptions;
import com.typesafe.config.ConfigRenderOptions;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgnitionManager;
import org.apache.ignite.internal.app.EnvironmentDefaultValueProvider;
import org.apache.ignite.network.NetworkAddress;
import picocli.CommandLine;
import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.TypeConversionException;

/**
 * The main entry point for run new Ignite node from CLI toolchain.
 */
@SuppressWarnings("FieldMayBeFinal")
@Command(name = "runner")
public class IgniteCliRunner implements Callable<CompletableFuture<Ignite>> {

    @ArgGroup
    private ConfigOptions configOptions = new ConfigOptions();

    private static class ConfigOptions {
        @Option(names = {"--config-path"}, description = "Path to node configuration file in HOCON format.")
        private Path configPath;

        @Option(names = {"--config-string"}, description = "Node configuration in HOCON format.")
        private String configString;
    }

    @ArgGroup(validate = false)
    private ConfigArgs configArgs = new ConfigArgs();

    private static class ConfigArgs {
        @Option(names = {"-p", "--port"}, description = "Node port.")
        private Integer port;

        @Option(names = {"-r", "--rest-port"}, description = "REST port.")
        private Integer restPort;

        @Option(names = {"-j", "--join"}, description = "Seed nodes.", split = ",")
        private NetworkAddress[] seedNodes;

        // picocli doesn't apply default values to arg groups without initial value, so we can't test for configArgs == null
        // and need to test all args separately.
        private boolean isEmpty() {
            return port == null && restPort == null && seedNodes == null;
        }
    }

    @Option(names = {"--work-dir"}, description = "Path to node working directory.", required = true)
    private Path workDir;

    @Option(names = {"--node-name"}, description = "Node name.", required = true)
    private String nodeName;

    @Override
    public CompletableFuture<Ignite> call() throws Exception {
        Path configPath = configOptions.configPath;
        // If config path is specified and there are no overrides then pass it directly.
        // picocli doesn't apply default values to group args, so we need to test all args separately.
        if (configPath != null && configArgs.isEmpty()) {
            return IgnitionManager.start(nodeName, configPath.toAbsolutePath(), workDir, null);
        }
        return IgnitionManager.start(nodeName, getConfigStr(), workDir);
    }

    private String getConfigStr() {
        Config configOptions = parseConfigOptions();
        Config configArgs = parseConfigArgs();
        // Override config from file or string with command-line arguments
        Config config = configArgs.withFallback(configOptions).resolve();
        return config.isEmpty() ? null : config.root().render(ConfigRenderOptions.concise().setJson(false));
    }

    private Config parseConfigOptions() {
        Path configPath = configOptions.configPath;
        String configString = configOptions.configString;
        if (configPath != null) {
            return ConfigFactory.parseFile(configPath.toFile(), ConfigParseOptions.defaults().setAllowMissing(false));
        } else if (configString != null) {
            return ConfigFactory.parseString(configString);
        }
        return ConfigFactory.empty();
    }

    private Config parseConfigArgs() {
        Map<String, Object> configMap = new HashMap<>();
        if (configArgs.port != null) {
            configMap.put("network.port", configArgs.port);
        }
        if (configArgs.seedNodes != null) {
            List<String> strings = Arrays.stream(configArgs.seedNodes)
                    .map(NetworkAddress::toString)
                    .collect(Collectors.toList());
            configMap.put("network.nodeFinder.netClusterNodes", strings);
        }
        if (configArgs.restPort != null) {
            configMap.put("rest.port", configArgs.restPort);
        }
        return ConfigFactory.parseMap(configMap);
    }

    // Used only for testing, may be simplified later.
    public static CompletableFuture<Ignite> start(String... args) {
        CommandLine commandLine = new CommandLine(new IgniteCliRunner());
        commandLine.setDefaultValueProvider(new EnvironmentDefaultValueProvider());
        commandLine.registerConverter(NetworkAddress.class, value -> {
            try {
                return NetworkAddress.from(value);
            } catch (IllegalArgumentException e) {
                throw new TypeConversionException(e.getMessage());
            }
        });
        int exitCode = commandLine.execute(args);
        if (exitCode != 0) {
            System.exit(exitCode);
        }
        return commandLine.getExecutionResult();
    }

    /**
     * Main method for running a new Ignite node.
     *
     * @param args CLI args to start a new node.
     */
    public static void main(String[] args) {
        try {
            start(args).get();
        } catch (ExecutionException | InterruptedException e) {
            System.out.println("Error when starting the node: " + e.getMessage());

            e.printStackTrace(System.out);

            System.exit(1);
        }
    }
}
