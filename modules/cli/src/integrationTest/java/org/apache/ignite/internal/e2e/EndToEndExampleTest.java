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

package org.apache.ignite.internal.e2e;

import static java.util.stream.Collectors.toList;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.testNodeName;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.emptyString;
import static org.hamcrest.Matchers.not;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.IntStream;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgnitionManager;
import org.apache.ignite.internal.testframework.WorkDirectory;
import org.apache.ignite.internal.testframework.WorkDirectoryExtension;
import org.apache.ignite.internal.util.IgniteUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;

@ExtendWith(WorkDirectoryExtension.class)
public class EndToEndExampleTest {
    /** Start network port for test nodes. */
    private static final int BASE_PORT = 3344;

    private final List<String> clusterNodeNames = new ArrayList<>();

    private final List<CompletableFuture<Ignite>> nodesFutures = new ArrayList<>();

    @WorkDirectory
    Path workDir;

    Cli cli;

    @AfterEach
    void tearDown() throws Exception {
        List<AutoCloseable> closeables = clusterNodeNames.stream()
                .map(name -> (AutoCloseable) () -> IgnitionManager.stop(name))
                .collect(toList());

        IgniteUtils.closeAll(closeables);
        cli.run("exit").assertThatOutput(not(emptyString())); // todo add wait
        cli.stop();
    }

    @BeforeEach
    void setUp(TestInfo testInfo, @TempDir Path tmpDir) {
        startIgniteNode(testInfo, 1);
        cli = Cli.startInteractive(tmpDir, System.out::println);
        cli.run("cp -r /Users/apakhomov/unison/ignite-3/packaging/build/distributions/ignite3-cli-3.0.0-SNAPSHOT .")
                .run("cd ignite3-cli-3.0.0-SNAPSHOT");
    }

    @Test
    @DisplayName("Can connect to already running node")
    void suggestToConnect() {
        cli.run("sh bin/ignite3")
                .assertThatOutput(
                        containsString("Do you want to reconnect to the last connected node"))
                .run("Y")
                .assertThatOutput(
                        containsString("Connected to"))
                .run("node config show")
                .assertThatOutput(containsString("rest"))
                .run("node config show rest.port")
                .assertThatOutput(containsString("10300"));
    }

    private void startIgniteNode(TestInfo testInfo, int nodesCount) {
        List<CompletableFuture<Ignite>> futures = IntStream.range(0, nodesCount)
                .mapToObj(i -> startNodeAsync(testInfo, i))
                .collect(toList());

        nodesFutures.addAll(futures);
    }

    private CompletableFuture<Ignite> startNodeAsync(TestInfo testInfo, int index) {
        String nodeName = testNodeName(testInfo, BASE_PORT + index);

        clusterNodeNames.add(nodeName);

        return IgnitionManager.start(nodeName, buildConfig(index), workDir.resolve(nodeName));
    }

    private String buildConfig(int nodeIdx) {
        return "{\n"
                + "  network: {\n"
                + "    port: " + (BASE_PORT + nodeIdx) + ",\n"
                + "    portRange: 1,\n"
                + "    nodeFinder: {\n"
                + "      netClusterNodes: [ \"localhost:3344\", \"localhost:3345\", \"localhost:3346\" ] \n"
                + "    }\n"
                + "  }\n"
                + "}";
    }
}
