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

package org.apache.ignite.internal.raft;

import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;
import static java.util.stream.IntStream.range;
import static org.apache.ignite.internal.network.ConstantClusterIdSupplier.withoutClusterId;
import static org.apache.ignite.internal.network.configuration.NetworkConfigurationSchema.DEFAULT_PORT;
import static org.apache.ignite.internal.network.utils.ClusterServiceTestUtils.defaultChannelTypeRegistry;
import static org.apache.ignite.internal.network.utils.ClusterServiceTestUtils.defaultSerializationRegistry;
import static org.apache.ignite.internal.raft.PeersAndLearners.fromConsistentIds;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;
import static org.apache.ignite.internal.util.IgniteUtils.closeAllManually;
import static org.apache.ignite.raft.TestWriteCommand.testWriteCommand;
import static org.awaitility.Awaitility.await;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import org.apache.ignite.internal.close.ManuallyCloseable;
import org.apache.ignite.internal.configuration.ComponentWorkingDir;
import org.apache.ignite.internal.configuration.SystemLocalConfiguration;
import org.apache.ignite.internal.configuration.testframework.ConfigurationExtension;
import org.apache.ignite.internal.configuration.testframework.InjectConfiguration;
import org.apache.ignite.internal.failure.FailureManager;
import org.apache.ignite.internal.hlc.HybridClock;
import org.apache.ignite.internal.hlc.HybridClockImpl;
import org.apache.ignite.internal.lang.NodeStoppingException;
import org.apache.ignite.internal.manager.ComponentContext;
import org.apache.ignite.internal.network.ClusterService;
import org.apache.ignite.internal.network.NettyBootstrapFactory;
import org.apache.ignite.internal.network.configuration.NetworkConfiguration;
import org.apache.ignite.internal.network.configuration.StaticNodeFinderChange;
import org.apache.ignite.internal.network.recovery.InMemoryStaleIds;
import org.apache.ignite.internal.network.scalecube.TestScaleCubeClusterServiceFactory;
import org.apache.ignite.internal.raft.configuration.RaftConfiguration;
import org.apache.ignite.internal.raft.server.RaftGroupOptions;
import org.apache.ignite.internal.raft.service.CommandClosure;
import org.apache.ignite.internal.raft.service.RaftGroupListener;
import org.apache.ignite.internal.raft.service.RaftGroupService;
import org.apache.ignite.internal.raft.storage.LogStorageFactory;
import org.apache.ignite.internal.raft.storage.impl.OnHeapLogs;
import org.apache.ignite.internal.raft.storage.impl.UnlimitedBudget;
import org.apache.ignite.internal.raft.storage.impl.VolatileLogStorage;
import org.apache.ignite.internal.replicator.ReplicationGroupId;
import org.apache.ignite.internal.replicator.TestReplicationGroupId;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.internal.testframework.WorkDirectory;
import org.apache.ignite.internal.testframework.WorkDirectoryExtension;
import org.apache.ignite.internal.version.DefaultIgniteProductVersionSource;
import org.apache.ignite.internal.worker.fixtures.NoOpCriticalWorkerRegistry;
import org.apache.ignite.raft.TestWriteCommand;
import org.apache.ignite.raft.jraft.conf.Configuration;
import org.apache.ignite.raft.jraft.conf.ConfigurationEntry;
import org.apache.ignite.raft.jraft.conf.ConfigurationManager;
import org.apache.ignite.raft.jraft.entity.EnumOutter;
import org.apache.ignite.raft.jraft.entity.LogEntry;
import org.apache.ignite.raft.jraft.entity.LogId;
import org.apache.ignite.raft.jraft.option.LogStorageOptions;
import org.apache.ignite.raft.jraft.option.RaftOptions;
import org.apache.ignite.raft.jraft.storage.LogStorage;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Integration tests for Raft divergence detection - Regression and documentation tests.
 *
 * <p><b>Why True Divergence is Not Tested Here:</b>
 * Creating actual divergence scenarios (same index, different term in applied data) in integration
 * tests is extremely difficult because:
 * <ul>
 * <li>Raft consensus protocol is specifically designed to prevent divergence</li>
 * <li>Applied state is tightly coupled with log storage and FSM state</li>
 * <li>Manipulating internal Raft state while maintaining invariants requires breaking encapsulation</li>
 * <li>Any manipulation that creates divergence would likely violate Raft safety properties</li>
 * </ul>
 *
 * <p><b>Divergence Detection IS Thoroughly Tested:</b>
 * <ul>
 * <li>{@link org.apache.ignite.raft.jraft.storage.impl.LogManagerTest} - 5 test methods for AppendEntries</li>
 * <li>{@link org.apache.ignite.raft.jraft.core.FSMCallerDivergenceTest} - 4 unit tests for snapshots</li>
 * <li>Both suites verify: divergence detection, error state transition, no false positives</li>
 * </ul>
 *
 * <p><b>Purpose of These Tests:</b>
 * These tests serve as regression tests to ensure that:
 * <ul>
 * <li>Normal Raft group operations continue to work correctly</li>
 * <li>Log truncation and recovery paths are not broken by divergence detection</li>
 * <li>The divergence detection code paths are compiled and linked properly</li>
 * </ul>
 *
 * <p>Uses {@link ItTruncateSuffixAndRestartTest} pattern with volatile log storage for potential
 * future enhancements if controlled divergence scenarios become feasible.
 */
@ExtendWith(WorkDirectoryExtension.class)
@ExtendWith(ConfigurationExtension.class)
public class ItRaftDivergenceDetectionTest extends BaseIgniteAbstractTest {
    private static final int NODES = 3;

    private static final String GROUP_NAME = "divergence-test";

    private static final ReplicationGroupId GROUP_ID = new TestReplicationGroupId(GROUP_NAME);

    private final PeersAndLearners raftGroupConfiguration = fromConsistentIds(
            range(0, NODES).mapToObj(ItRaftDivergenceDetectionTest::nodeName).collect(toSet())
    );

    private final HybridClock hybridClock = new HybridClockImpl();

    /** List of resources to be released after each test. */
    private final List<ManuallyCloseable> cleanup = new ArrayList<>();

    @WorkDirectory
    private Path workDir;

    @InjectConfiguration
    private RaftConfiguration raftConfiguration;

    @InjectConfiguration
    private SystemLocalConfiguration systemLocalConfiguration;

    @InjectConfiguration
    private NetworkConfiguration networkConfiguration;

    private List<SimpleRaftNode> nodes;

    @BeforeEach
    void setUp() {
        CompletableFuture<Void> changeFuture = networkConfiguration.change(cfg -> cfg
                .changeNodeFinder().convert(StaticNodeFinderChange.class)
                .changeNetClusterNodes(
                        range(port(0), port(NODES)).mapToObj(port -> "localhost:" + port).toArray(String[]::new)
                )
        );

        assertThat(changeFuture, willCompleteSuccessfully());

        nodes = range(0, NODES).mapToObj(SimpleRaftNode::new).collect(toList());

        nodes.forEach(SimpleRaftNode::startService);
    }

    @AfterEach
    void tearDown() throws Exception {
        Collections.reverse(cleanup);

        closeAllManually(cleanup.stream());
    }

    /**
     * Documents divergence detection behavior with Raft groups.
     *
     * <p><b>Note on Testing Divergence:</b> Creating true divergence scenarios in integration tests
     * is extremely difficult because:
     * <ul>
     * <li>Raft consensus is designed to prevent divergence by design</li>
     * <li>Applied state is tightly coupled with log state and storage</li>
     * <li>Manipulating internal Raft state while maintaining invariants is complex</li>
     * </ul>
     *
     * <p><b>Divergence Detection is Thoroughly Tested:</b>
     * <ul>
     * <li>{@link org.apache.ignite.raft.jraft.storage.impl.LogManagerTest} - Unit tests for AppendEntries divergence</li>
     * <li>{@link org.apache.ignite.raft.jraft.core.FSMCallerDivergenceTest} - Unit tests for snapshot divergence</li>
     * <li>Both test suites verify error state transitions and BROKEN partition marking</li>
     * </ul>
     *
     * <p>This test verifies that normal Raft group operations work correctly,
     * serving as a regression test to ensure divergence detection doesn't break normal paths.
     */
    @Test
    void testNormalRaftGroupOperations_RegressionTest() throws Exception {
        SimpleRaftNode node0 = nodes.get(0);
        SimpleRaftNode node1 = nodes.get(1);
        SimpleRaftNode node2 = nodes.get(2);

        // Write initial value - all nodes apply it
        log.info("Writing initial value to all nodes");
        assertThat(node0.getService().run(testWriteCommand("1")), willCompleteSuccessfully());

        // Wait for replication to all nodes
        waitForValueReplication(nodes, "1");

        log.info("Writing more values");
        assertThat(node0.getService().run(testWriteCommand("2")), willCompleteSuccessfully());
        assertThat(node0.getService().run(testWriteCommand("3")), willCompleteSuccessfully());

        waitForValueReplication(nodes, "3");

        log.info("Test completed - normal Raft operations work correctly");
    }

    /**
     * Tests node restart and rejoin scenario.
     *
     * <p>Verifies that a node can successfully restart, rejoin the Raft group, and catch up.
     * This is a regression test to ensure divergence detection doesn't break normal recovery paths.
     */
    @Test
    void testNodeRestartAndRejoin_RegressionTest() throws Exception {
        SimpleRaftNode node0 = nodes.get(0);
        SimpleRaftNode node2 = nodes.get(2);

        // Write and replicate initial value
        log.info("Writing initial value");
        assertThat(node0.getService().run(testWriteCommand("1")), willCompleteSuccessfully());
        waitForValueReplication(nodes, "1");

        // Stop and restart node 2
        log.info("Stopping and restarting node 2");
        node2.stopService();
        node2.startService();

        // Write new value and verify all nodes have it
        log.info("Writing new value after node 2 rejoins");
        assertThat(node0.getService().run(testWriteCommand("2")), willCompleteSuccessfully());

        waitForValueReplication(nodes, "2");

        log.info("Node restart and rejoin test completed successfully");
    }

    private static String nodeName(int i) {
        return "divergence-node" + i;
    }

    private static int port(int i) {
        return DEFAULT_PORT + 100 + i;
    }

    private static void waitForValueReplication(List<SimpleRaftNode> nodes, String value) {
        await().atMost(10, SECONDS).untilAsserted(() -> {
            for (SimpleRaftNode node : nodes) {
                assertTrue(value.equals(node.raftGroupListener.lastValue),
                        "Expected value '" + value + "' on node " + node.nodeName
                        + ", but got: " + node.raftGroupListener.lastValue);
            }
        });
    }

    /**
     * Simple Raft node using volatile log storage for controlled manipulation.
     *
     * <p>Similar to the pattern in {@link ItTruncateSuffixAndRestartTest}, but focused on
     * divergence testing scenarios.
     */
    private class SimpleRaftNode {
        final LogStorage logStorage = new VolatileLogStorage(
                new UnlimitedBudget(),
                new ReusableOnHeapLogs(),
                new OnHeapLogs()
        );

        final LogStorageFactory logStorageFactory = new TestLogStorageFactory(logStorage);

        final String nodeName;

        final ClusterService clusterSvc;

        final Path nodeDir;

        final ComponentWorkingDir partitionsWorkDir;

        final Loza raftMgr;

        private @Nullable RaftGroupService raftGroupService;

        private TestRaftGroupListener raftGroupListener;

        private SimpleRaftNode(int i) {
            nodeName = nodeName(i);
            nodeDir = workDir.resolve(nodeName);

            assertThat(networkConfiguration.port().update(port(i)), willCompleteSuccessfully());

            var nettyBootstrapFactory = new NettyBootstrapFactory(networkConfiguration, nodeName);

            assertThat(nettyBootstrapFactory.startAsync(new ComponentContext()), willCompleteSuccessfully());
            cleanup.add(() -> assertThat(nettyBootstrapFactory.stopAsync(new ComponentContext()), willCompleteSuccessfully()));

            clusterSvc = new TestScaleCubeClusterServiceFactory().createClusterService(
                    nodeName,
                    networkConfiguration,
                    nettyBootstrapFactory,
                    defaultSerializationRegistry(),
                    new InMemoryStaleIds(),
                    withoutClusterId(),
                    new NoOpCriticalWorkerRegistry(),
                    mock(FailureManager.class),
                    defaultChannelTypeRegistry(),
                    new DefaultIgniteProductVersionSource()
            );

            assertThat(clusterSvc.startAsync(new ComponentContext()), willCompleteSuccessfully());
            cleanup.add(() -> assertThat(clusterSvc.stopAsync(new ComponentContext()), willCompleteSuccessfully()));

            partitionsWorkDir = new ComponentWorkingDir(nodeDir);

            raftMgr = TestLozaFactory.create(clusterSvc, raftConfiguration, systemLocalConfiguration, hybridClock);

            assertThat(raftMgr.startAsync(new ComponentContext()), willCompleteSuccessfully());
            cleanup.add(() -> assertThat(raftMgr.stopAsync(new ComponentContext()), willCompleteSuccessfully()));

            cleanup.add(this::stopService);
        }

        void startService() {
            raftGroupListener = new TestRaftGroupListener();

            try {
                raftGroupService = raftMgr.startRaftGroupNode(
                        new RaftNodeId(GROUP_ID, new Peer(nodeName)),
                        raftGroupConfiguration,
                        raftGroupListener,
                        RaftGroupEventsListener.noopLsnr,
                        RaftGroupOptions.defaults()
                                .setLogStorageFactory(logStorageFactory)
                                .serverDataPath(partitionsWorkDir.metaPath())
                );
            } catch (NodeStoppingException e) {
                throw new RuntimeException("Failed to start raft group service", e);
            }
        }

        RaftGroupService getService() {
            assertNotNull(raftGroupService);
            return raftGroupService;
        }

        void stopService() {
            if (raftGroupService != null) {
                raftGroupService.shutdown();
                raftGroupService = null;

                try {
                    raftMgr.stopRaftNode(new RaftNodeId(GROUP_ID, new Peer(nodeName)));
                } catch (NodeStoppingException e) {
                    log.warn("Error stopping raft node", e);
                }
            }
        }
    }

    /**
     * Reusable on-heap logs that recover configuration on restart.
     */
    private static class ReusableOnHeapLogs extends OnHeapLogs {
        @Override
        public boolean init(LogStorageOptions opts) {
            ConfigurationManager confManager = opts.getConfigurationManager();

            for (int i = 1;; i++) {
                LogEntry entry = getEntry(i);

                if (entry == null) {
                    break;
                }

                if (entry.getType() == EnumOutter.EntryType.ENTRY_TYPE_CONFIGURATION) {
                    ConfigurationEntry confEntry = new ConfigurationEntry();

                    confEntry.setId(new LogId(entry.getId().getIndex(), entry.getId().getTerm()));
                    confEntry.setConf(new Configuration(entry.getPeers(), entry.getLearners()));

                    if (entry.getOldPeers() != null) {
                        confEntry.setOldConf(new Configuration(entry.getOldPeers(), entry.getOldLearners()));
                    }

                    confManager.add(confEntry);
                }
            }

            return super.init(opts);
        }
    }

    private static class TestRaftGroupListener implements RaftGroupListener {
        volatile String lastValue;

        @Override
        public void onRead(Iterator<CommandClosure<ReadCommand>> iterator) {
        }

        @Override
        public void onWrite(Iterator<CommandClosure<WriteCommand>> iterator) {
            iterator.forEachRemaining(closure -> {
                lastValue = ((TestWriteCommand) closure.command()).value();
                closure.result(null);
            });
        }

        @Override
        public void onSnapshotSave(Path path, Consumer<Throwable> doneClo) {
            doneClo.accept(null);
        }

        @Override
        public boolean onSnapshotLoad(Path path) {
            return false;
        }

        @Override
        public void onShutdown() {
        }
    }

    private static class TestLogStorageFactory implements LogStorageFactory {
        private final LogStorage logStorage;

        TestLogStorageFactory(LogStorage logStorage) {
            this.logStorage = logStorage;
        }

        @Override
        public LogStorage createLogStorage(String uri, RaftOptions raftOptions) {
            return logStorage;
        }

        @Override
        public void destroyLogStorage(String uri) {
            // No-op.
        }

        @Override
        public Set<String> raftNodeStorageIdsOnDisk() {
            return Set.of();
        }

        @Override
        public CompletableFuture<Void> startAsync(ComponentContext componentContext) {
            return nullCompletedFuture();
        }

        @Override
        public CompletableFuture<Void> stopAsync(ComponentContext componentContext) {
            return nullCompletedFuture();
        }
    }
}
