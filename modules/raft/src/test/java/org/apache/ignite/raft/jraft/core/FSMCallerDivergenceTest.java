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
package org.apache.ignite.raft.jraft.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.internal.thread.IgniteThreadFactory;
import org.apache.ignite.raft.jraft.JRaftUtils;
import org.apache.ignite.raft.jraft.RaftMessagesFactory;
import org.apache.ignite.raft.jraft.StateMachine;
import org.apache.ignite.raft.jraft.Status;
import org.apache.ignite.raft.jraft.closure.ClosureQueueImpl;
import org.apache.ignite.raft.jraft.closure.LoadSnapshotClosure;
import org.apache.ignite.raft.jraft.disruptor.StripedDisruptor;
import org.apache.ignite.raft.jraft.entity.LogId;
import org.apache.ignite.raft.jraft.entity.NodeId;
import org.apache.ignite.raft.jraft.entity.PeerId;
import org.apache.ignite.raft.jraft.entity.RaftOutter.SnapshotMeta;
import org.apache.ignite.raft.jraft.error.RaftError;
import org.apache.ignite.raft.jraft.option.FSMCallerOptions;
import org.apache.ignite.raft.jraft.option.NodeOptions;
import org.apache.ignite.raft.jraft.storage.LogManager;
import org.apache.ignite.raft.jraft.storage.snapshot.SnapshotReader;
import org.apache.ignite.raft.jraft.util.ExecutorServiceHelper;
import org.apache.ignite.raft.jraft.util.Utils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

/**
 * Unit tests for FSMCaller divergence detection during snapshot installation.
 *
 * <p>Tests verify that when a node attempts to install a snapshot that would overwrite
 * divergent applied data (same index, different term), the FSMCaller correctly detects
 * this and transitions to error state instead of silently losing data.
 */
@ExtendWith(MockitoExtension.class)
public class FSMCallerDivergenceTest extends BaseIgniteAbstractTest {
    private FSMCallerImpl fsmCaller;
    private FSMCallerOptions opts;
    @Mock
    private NodeImpl node;
    @Mock
    private StateMachine fsm;
    @Mock
    private LogManager logManager;
    private ClosureQueueImpl closureQueue;

    /** Disruptor for this service test. */
    private StripedDisruptor disruptor;

    private ExecutorService executor;

    @BeforeEach
    public void setup() {
        this.fsmCaller = new FSMCallerImpl();
        NodeOptions options = new NodeOptions();
        executor = JRaftUtils.createExecutor("test-node", "test-executor-", Utils.cpus());
        options.setCommonExecutor(executor);
        this.closureQueue = new ClosureQueueImpl(options);
        opts = new FSMCallerOptions();
        Mockito.when(this.node.getNodeMetrics()).thenReturn(new NodeMetrics(false));
        Mockito.when(this.node.getOptions()).thenReturn(options);
        Mockito.when(this.node.getNodeId()).thenReturn(new NodeId("foo", new PeerId("bar")));
        opts.setNode(this.node);
        opts.setFsm(this.fsm);
        opts.setLogManager(this.logManager);
        opts.setBootstrapId(new LogId(10, 1));
        opts.setClosureQueue(this.closureQueue);
        opts.setRaftMessagesFactory(new RaftMessagesFactory());
        opts.setfSMCallerExecutorDisruptor(disruptor = new StripedDisruptor<>("test", "TestFSMDisruptor",
                (stripeName, logger) -> IgniteThreadFactory.create("test", stripeName, true, logger),
                1024,
                () -> new FSMCallerImpl.ApplyTask(),
                1,
                false,
                false,
                null));
        assertTrue(this.fsmCaller.init(opts));
    }

    @AfterEach
    public void teardown() throws Exception {
        if (this.fsmCaller != null) {
            this.fsmCaller.shutdown();
            this.fsmCaller.join();
            disruptor.shutdown();
        }
        ExecutorServiceHelper.shutdownAndAwaitTermination(executor);
    }

    /**
     * Tests that FSMCaller detects divergence when snapshot is at the same index but with a different term.
     *
     * <p>Scenario:
     * - Node has applied up to (index=10, term=1) from bootstrap
     * - Leader sends snapshot with (index=10, term=2) - same index, different term
     * - Expected: Divergence detected, error state set, snapshot rejected
     */
    @Test
    public void testSnapshotLoad_WithDivergentAppliedData_CallsSetError() throws Exception {
        final SnapshotReader reader = Mockito.mock(SnapshotReader.class);

        // Snapshot at same index (10) but different term (2) than applied (10, 1)
        final SnapshotMeta meta = opts.getRaftMessagesFactory()
            .snapshotMeta()
            .lastIncludedIndex(10)
            .lastIncludedTerm(2) // Different term!
            .build();
        Mockito.when(reader.load()).thenReturn(meta);

        final CountDownLatch latch = new CountDownLatch(1);
        this.fsmCaller.onSnapshotLoad(new LoadSnapshotClosure() {

            @Override
            public void run(final Status status) {
                // Verify error status
                assertFalse(status.isOk());
                assertEquals(RaftError.EINTERNAL, status.getRaftError());
                assertTrue(status.getErrorMsg().contains("would lose divergent applied data"));
                assertTrue(status.getErrorMsg().contains("same index but with different term"));
                latch.countDown();
            }

            @Override
            public SnapshotReader start() {
                return reader;
            }
        });
        latch.await();

        // Verify applied index not changed
        assertEquals(10, this.fsmCaller.getLastAppliedIndex());

        // Verify FSM in error state
        assertFalse(this.fsmCaller.getError().getStatus().isOk());
        assertTrue(this.fsmCaller.getError().getStatus().getErrorMsg().contains("divergent applied data"));
    }

    /**
     * Tests that FSMCaller loads snapshot successfully when it's at a higher index with consistent history.
     *
     * <p>Scenario:
     * - Node has applied up to (index=10, term=1) from bootstrap
     * - Leader sends snapshot with (index=12, term=1) - extends applied history
     * - Expected: Snapshot loads successfully, lastAppliedIndex updated to 12
     */
    @Test
    public void testSnapshotLoad_WithValidSnapshot_LoadsSuccessfully() throws Exception {
        final SnapshotReader reader = Mockito.mock(SnapshotReader.class);

        // Valid snapshot at higher index
        final SnapshotMeta meta = opts.getRaftMessagesFactory()
            .snapshotMeta()
            .lastIncludedIndex(12)
            .lastIncludedTerm(1) // Same term, no divergence
            .build();
        Mockito.when(reader.load()).thenReturn(meta);
        Mockito.when(this.fsm.onSnapshotLoad(reader)).thenReturn(true);

        final CountDownLatch latch = new CountDownLatch(1);
        this.fsmCaller.onSnapshotLoad(new LoadSnapshotClosure() {

            @Override
            public void run(final Status status) {
                // Verify success
                assertTrue(status.isOk());
                latch.countDown();
            }

            @Override
            public SnapshotReader start() {
                return reader;
            }
        });
        latch.await();

        // Verify applied index updated
        assertEquals(12, this.fsmCaller.getLastAppliedIndex());

        // Verify no error state
        assertTrue(this.fsmCaller.getError().getStatus().isOk());
    }

    /**
     * Tests that FSMCaller loads snapshot successfully when terms match exactly.
     *
     * <p>Scenario:
     * - Node has applied up to (index=10, term=1) from bootstrap
     * - Leader sends snapshot with (index=10, term=1) - exact match
     * - Expected: Snapshot loads successfully (idempotent operation)
     */
    @Test
    public void testSnapshotLoad_WithMatchingTermAtSameIndex_LoadsSuccessfully() throws Exception {
        final SnapshotReader reader = Mockito.mock(SnapshotReader.class);

        // Snapshot at same index and same term - exact match
        final SnapshotMeta meta = opts.getRaftMessagesFactory()
            .snapshotMeta()
            .lastIncludedIndex(10)
            .lastIncludedTerm(1) // Matches applied term
            .build();
        Mockito.when(reader.load()).thenReturn(meta);
        Mockito.when(this.fsm.onSnapshotLoad(reader)).thenReturn(true);

        final CountDownLatch latch = new CountDownLatch(1);
        this.fsmCaller.onSnapshotLoad(new LoadSnapshotClosure() {

            @Override
            public void run(final Status status) {
                // Verify success
                assertTrue(status.isOk());
                latch.countDown();
            }

            @Override
            public SnapshotReader start() {
                return reader;
            }
        });
        latch.await();

        // Verify applied index unchanged (already at 10)
        assertEquals(10, this.fsmCaller.getLastAppliedIndex());

        // Verify no error state
        assertTrue(this.fsmCaller.getError().getStatus().isOk());
    }

    /**
     * Tests divergence detection boundary case: higher index with higher term.
     *
     * <p>Scenario:
     * - Node has applied up to (index=10, term=1) from bootstrap
     * - Leader sends snapshot with (index=15, term=2) - both index and term higher
     * - Expected: Snapshot loads successfully (normal progression, no divergence at applied index)
     */
    @Test
    public void testSnapshotLoad_WithHigherIndexAndTerm_LoadsSuccessfully() throws Exception {
        final SnapshotReader reader = Mockito.mock(SnapshotReader.class);

        // Snapshot with both higher index and term
        final SnapshotMeta meta = opts.getRaftMessagesFactory()
            .snapshotMeta()
            .lastIncludedIndex(15)
            .lastIncludedTerm(2)
            .build();
        Mockito.when(reader.load()).thenReturn(meta);
        Mockito.when(this.fsm.onSnapshotLoad(reader)).thenReturn(true);

        final CountDownLatch latch = new CountDownLatch(1);
        this.fsmCaller.onSnapshotLoad(new LoadSnapshotClosure() {

            @Override
            public void run(final Status status) {
                // Verify success
                assertTrue(status.isOk());
                latch.countDown();
            }

            @Override
            public SnapshotReader start() {
                return reader;
            }
        });
        latch.await();

        // Verify applied index updated
        assertEquals(15, this.fsmCaller.getLastAppliedIndex());

        // Verify no error state
        assertTrue(this.fsmCaller.getError().getStatus().isOk());
    }
}
