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

package org.apache.ignite.internal.cli.commands;

import static org.apache.ignite.internal.testframework.IgniteTestUtils.waitForCondition;
import static org.junit.jupiter.api.Assertions.assertAll;

import java.time.Duration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/** Tests for ignite node commands with a provided node name. */
public class NodeNameTest extends CliCommandTestNotInitializedIntegrationBase {

    private String nodeName;

    @BeforeEach
    void setUp() throws InterruptedException {
        nodeNameRegistry.startPullingUpdates("http://localhost:10301");
        // wait to pulling node names
        waitForCondition(() -> !nodeNameRegistry.getAllNames().isEmpty(), Duration.ofSeconds(5).toMillis());
        this.nodeName = nodeNameRegistry.getAllNames().stream()
                .findFirst()
                .orElseThrow(() -> new RuntimeException("nodeNameRegistry doesn't contain any nodes"));
    }

    @Test
    @DisplayName("Should display node version with provided node name")
    void nodeVersion() {
        // When
        execute("node", "version", "--node-name", nodeName);

        // Then
        assertAll(
                this::assertExitCodeIsZero,
                this::assertErrOutputIsEmpty,
                () -> assertOutputMatches("[1-9]\\d*\\.\\d+\\.\\d+(?:-[a-zA-Z0-9]+)?\\s+")
        );
    }

    @Test
    @DisplayName("Should display node config with provided node name")
    void nodeConfig() {
        // When
        execute("node", "config", "show", "--node-name", nodeName);

        // Then
        assertAll(
                this::assertExitCodeIsZero,
                this::assertErrOutputIsEmpty,
                this::assertOutputIsNotEmpty
        );
    }

    @Test
    @DisplayName("Should display node status with provided node name")
    void nodeStatus() {
        // When
        execute("node", "status", "--node-name", nodeName);

        // Then
        assertAll(
                this::assertExitCodeIsZero,
                this::assertErrOutputIsEmpty,
                () -> assertOutputMatches("\\[name: " + nodeName + ", state: starting\\]?\\s+")
        );
    }

    @AfterEach
    void tearDown() {
        nodeNameRegistry.stopPullingUpdates();
    }
}
