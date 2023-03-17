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

package org.apache.ignite.internal.network.netty;

import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.apache.ignite.network.ChannelInfo.defaultChannel;
import static org.apache.ignite.utils.ClusterServiceTestUtils.defaultSerializationRegistry;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.isA;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyShort;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.netty.handler.codec.DecoderException;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.ClosedChannelException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.internal.configuration.testframework.ConfigurationExtension;
import org.apache.ignite.internal.configuration.testframework.InjectConfiguration;
import org.apache.ignite.internal.future.OrderingFuture;
import org.apache.ignite.internal.network.configuration.NetworkConfiguration;
import org.apache.ignite.internal.network.configuration.NetworkView;
import org.apache.ignite.internal.network.messages.EmptyMessageImpl;
import org.apache.ignite.internal.network.messages.TestMessage;
import org.apache.ignite.internal.network.messages.TestMessagesFactory;
import org.apache.ignite.internal.network.serialization.SerializationService;
import org.apache.ignite.internal.network.serialization.UserObjectSerializationContext;
import org.apache.ignite.internal.testframework.IgniteTestUtils;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.lang.IgniteInternalException;
import org.apache.ignite.network.ChannelInfo;
import org.apache.ignite.network.NettyBootstrapFactory;
import org.apache.ignite.network.NetworkMessage;
import org.apache.ignite.network.OutNetworkObject;
import org.apache.ignite.network.serialization.MessageSerializationRegistry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Tests for {@link ConnectionManager}.
 */
@ExtendWith(ConfigurationExtension.class)
public class ItConnectionManagerTest {
    /** Started connection managers. */
    private final List<ConnectionManagerWrapper> startedManagers = new ArrayList<>();

    /** Message factory. */
    private final TestMessagesFactory messageFactory = new TestMessagesFactory();

    /** Reusable network configuration object. */
    @InjectConfiguration
    private NetworkConfiguration networkConfiguration;

    private static ChannelInfo testChannel;

    /**
     * After each.
     */
    @AfterEach
    final void tearDown() throws Exception {
        IgniteUtils.closeAll(startedManagers);
    }

    @BeforeAll
    static void registerChannel() {
        testChannel = ChannelInfo.generate("ItConnectionManagerTest");
    }

    /**
     * Tests that a message is sent successfully using the ConnectionManager.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testSentSuccessfully() throws Exception {
        String msgText = "test";

        int port1 = 4000;
        int port2 = 4001;


        try (ConnectionManagerWrapper manager1 = startManager(port1);
                ConnectionManagerWrapper manager2 = startManager(port2)) {
            var fut = new CompletableFuture<NetworkMessage>();

            manager2.connectionManager.addListener((obj) -> fut.complete(obj.message()));

            NettySender sender = manager1.openChannelTo(manager2).get(3, TimeUnit.SECONDS);

            TestMessage testMessage = messageFactory.testMessage().msg(msgText).build();

            sender.send(new OutNetworkObject(testMessage, Collections.emptyList())).get(3, TimeUnit.SECONDS);

            NetworkMessage receivedMessage = fut.get(3, TimeUnit.SECONDS);

            assertEquals(msgText, ((TestMessage) receivedMessage).msg());
        }
    }

    /**
     * Tests that incoming connection is reused for sending messages.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testReuseIncomingConnection() throws Exception {
        final String msgText = "test";

        TestMessage testMessage = messageFactory.testMessage().msg("test").build();

        int port1 = 4000;
        int port2 = 4001;

        try (ConnectionManagerWrapper manager1 = startManager(port1);
                ConnectionManagerWrapper manager2 = startManager(port2)) {
            var fut = new CompletableFuture<NetworkMessage>();

            manager1.connectionManager.addListener((obj) -> fut.complete(obj.message()));

            NettySender senderFrom1to2 = manager1.openChannelTo(manager2).get(3, TimeUnit.SECONDS);

            // Ensure a handshake has finished on both sides by sending a message.
            // TODO: IGNITE-16947 When the recovery protocol is implemented replace this with simple
            // CompletableFuture#get called on the send future.
            var messageReceivedOn2 = new CompletableFuture<Void>();

            // If the message is received, that means that the handshake was successfully performed.
            manager2.connectionManager.addListener((message) -> messageReceivedOn2.complete(null));

            senderFrom1to2.send(new OutNetworkObject(testMessage, Collections.emptyList()));

            messageReceivedOn2.get(3, TimeUnit.SECONDS);

            NettySender senderFrom2to1 = manager2.openChannelTo(manager1).get(3, TimeUnit.SECONDS);

            InetSocketAddress clientLocalAddress = (InetSocketAddress) senderFrom1to2.channel().localAddress();

            InetSocketAddress clientRemoteAddress = (InetSocketAddress) senderFrom2to1.channel().remoteAddress();

            assertEquals(clientLocalAddress, clientRemoteAddress);

            senderFrom2to1.send(new OutNetworkObject(testMessage, Collections.emptyList())).get(3, TimeUnit.SECONDS);

            NetworkMessage receivedMessage = fut.get(3, TimeUnit.SECONDS);

            assertEquals(msgText, ((TestMessage) receivedMessage).msg());
        }
    }

    /**
     * Tests that the resources of a connection manager are closed after a shutdown.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testShutdown() throws Exception {
        int port1 = 4000;
        int port2 = 4001;

        ConnectionManagerWrapper manager1 = startManager(port1);
        ConnectionManagerWrapper manager2 = startManager(port2);

        NettySender sender1 = manager1.openChannelTo(manager2).get(3, TimeUnit.SECONDS);
        NettySender sender2 = manager2.openChannelTo(manager1).get(3, TimeUnit.SECONDS);

        assertNotNull(sender1);
        assertNotNull(sender2);

        for (ConnectionManagerWrapper manager : List.of(manager1, manager2)) {
            NettyServer server = manager.connectionManager.server();
            Collection<NettyClient> clients = manager.connectionManager.clients();

            manager.close();

            assertFalse(server.isRunning());

            boolean clientsStopped = clients.stream().allMatch(NettyClient::isDisconnected);

            assertTrue(clientsStopped);
        }
    }

    /**
     * Tests that after a channel was closed, a new channel is opened upon a request.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testCanReconnectAfterFail() throws Exception {
        String msgText = "test";

        int port1 = 4000;
        int port2 = 4001;

        ConnectionManagerWrapper manager1 = startManager(port1);

        ConnectionManagerWrapper manager2 = startManager(port2);

        NettySender sender = manager1.openChannelTo(manager2).get(3, TimeUnit.SECONDS);

        TestMessage testMessage = messageFactory.testMessage().msg(msgText).build();

        manager2.close();

        NettySender finalSender = sender;

        assertThrows(ClosedChannelException.class, () -> {
            try {
                finalSender.send(new OutNetworkObject(testMessage, Collections.emptyList())).get(3, TimeUnit.SECONDS);
            } catch (Exception e) {
                throw e.getCause();
            }
        });

        manager2 = startManager(port2);

        var fut = new CompletableFuture<NetworkMessage>();

        manager2.connectionManager.addListener((obj) -> fut.complete(obj.message()));

        sender = manager1.openChannelTo(manager2).get(3, TimeUnit.SECONDS);

        sender.send(new OutNetworkObject(testMessage, Collections.emptyList())).get(3, TimeUnit.SECONDS);

        NetworkMessage receivedMessage = fut.get(3, TimeUnit.SECONDS);

        assertEquals(msgText, ((TestMessage) receivedMessage).msg());
    }

    /**
     * Tests that a connection to a misconfigured server results in a connection close and an exception on the client side.
     */
    @Test
    public void testConnectMisconfiguredServer() throws Exception {
        ConnectionManagerWrapper client = startManager(4000);

        try (ConnectionManagerWrapper server = startManager(4001, mockSerializationRegistry())) {
            client.openChannelTo(server).get(3, TimeUnit.SECONDS);
        } catch (ExecutionException e) {
            assertThat(e.getCause(), isA(IOException.class));
        }
    }

    /**
     * Tests that a connection from a misconfigured client results in an exception.
     */
    @Test
    public void testConnectMisconfiguredClient() throws Exception {
        ConnectionManagerWrapper client = startManager(4000, mockSerializationRegistry());

        try (ConnectionManagerWrapper server = startManager(4001)) {
            client.openChannelTo(server).get(3, TimeUnit.SECONDS);
        } catch (ExecutionException e) {
            assertThat(e.getCause(), isA(DecoderException.class));
        }
    }

    /**
     * Tests that a connection manager fails to start twice.
     */
    @Test
    public void testStartTwice() throws Exception {
        ConnectionManagerWrapper server = startManager(4000);

        assertThrows(IgniteInternalException.class, server.connectionManager::start);
    }

    /**
     * Tests that a connection manager can be stopped twice.
     */
    @Test
    public void testStopTwice() throws Exception {
        ConnectionManagerWrapper server = startManager(4000);

        server.close();
        server.close();
    }

    @Test
    public void testOneConnectionType() throws Exception {
        String bigText = IgniteTestUtils.randomString(new Random(), 10000000);

        try (ConnectionManagerWrapper server1 = startManager(4000);
                ConnectionManagerWrapper server2 = startManager(4001)) {
            NettySender sender = server1.openChannelTo(server2).get(3, TimeUnit.SECONDS);

            TestMessage bigMessage = messageFactory.testMessage().msg(bigText).build();
            TestMessage msg = messageFactory.testMessage().msg("test").build();

            CompletableFuture<Void> send1 = sender.send(new OutNetworkObject(bigMessage, Collections.emptyList()));
            CompletableFuture<Void> send2 = sender.send(new OutNetworkObject(msg, Collections.emptyList()));

            assertThat(send2, willCompleteSuccessfully());
            assertTrue(send1.isDone());
        }
    }

    @Test
    public void testTwoConnectionTypes() throws Exception {
        String bigText = IgniteTestUtils.randomString(new Random(), 100000000);

        Map<Integer, String> map = Map.of(1, bigText, 2, bigText);

        try (ConnectionManagerWrapper server1 = startManager(4000);
                ConnectionManagerWrapper server2 = startManager(4001)) {

            NettySender sender1 = server1.openChannelTo(server2).get(3, TimeUnit.SECONDS);
            NettySender sender2 = server1.openChannelTo(server2, testChannel).get(3, TimeUnit.SECONDS);

            TestMessage bigMessage = messageFactory.testMessage().msg(bigText).map(map).build();

            CompletableFuture<Void> send1 = sender1.send(new OutNetworkObject(bigMessage, Collections.emptyList()));
            CompletableFuture.runAsync(() -> {
                for (int i = 0; i < 100; i++) {
                    sender2.send(new OutNetworkObject(messageFactory.emptyMessage().build(), Collections.emptyList()));
                }
            });

            AtomicBoolean atLeastOneSmallWas = new AtomicBoolean(false);
            server2.connectionManager.addListener(inNetworkObject -> {
                System.out.println(inNetworkObject.message().groupType());
                if (inNetworkObject.message().groupType() == EmptyMessageImpl.GROUP_TYPE
                        && inNetworkObject.message().messageType() == EmptyMessageImpl.TYPE) {
                    atLeastOneSmallWas.set(true);
                }
            });

            assertThat(send1, willCompleteSuccessfully());
            assertTrue(atLeastOneSmallWas.get());
        }
    }

    /**
     * Creates a mock {@link MessageSerializationRegistry} that throws an exception when trying to get a serializer or a deserializer.
     */
    private static MessageSerializationRegistry mockSerializationRegistry() {
        var mockRegistry = mock(MessageSerializationRegistry.class);

        when(mockRegistry.createDeserializer(anyShort(), anyShort())).thenThrow(RuntimeException.class);
        when(mockRegistry.createSerializer(anyShort(), anyShort())).thenThrow(RuntimeException.class);

        return mockRegistry;
    }

    /**
     * Creates and starts a {@link ConnectionManager} listening on the given port.
     *
     * @param port Port for the connection manager to listen on.
     * @return Connection manager.
     */
    private ConnectionManagerWrapper startManager(int port) throws Exception {
        return startManager(port, defaultSerializationRegistry());
    }

    /**
     * Creates and starts a {@link ConnectionManager} listening on the given port, configured with the provided serialization registry.
     *
     * @param port     Port for the connection manager to listen on.
     * @param registry Serialization registry.
     * @return Connection manager.
     */
    private ConnectionManagerWrapper startManager(int port, MessageSerializationRegistry registry) throws Exception {
        UUID launchId = UUID.randomUUID();
        String consistentId = UUID.randomUUID().toString();

        networkConfiguration.port().update(port).join();

        NetworkView cfg = networkConfiguration.value();

        NettyBootstrapFactory bootstrapFactory = new NettyBootstrapFactory(networkConfiguration, consistentId);

        bootstrapFactory.start();

        try {
            var manager = new ConnectionManager(
                    cfg,
                    new SerializationService(registry, mock(UserObjectSerializationContext.class)),
                    launchId,
                    consistentId,
                    bootstrapFactory
            );

            manager.start();

            var wrapper = new ConnectionManagerWrapper(manager, bootstrapFactory);

            startedManagers.add(wrapper);

            return wrapper;
        } catch (Exception e) {
            bootstrapFactory.stop();

            throw e;
        }
    }

    private static class ConnectionManagerWrapper implements AutoCloseable {
        final ConnectionManager connectionManager;

        private final NettyBootstrapFactory nettyFactory;

        ConnectionManagerWrapper(ConnectionManager connectionManager, NettyBootstrapFactory nettyFactory) {
            this.connectionManager = connectionManager;
            this.nettyFactory = nettyFactory;
        }

        @Override
        public void close() throws Exception {
            IgniteUtils.closeAll(connectionManager::stop, nettyFactory::stop);
        }

        OrderingFuture<NettySender> openChannelTo(ConnectionManagerWrapper recipient) {
            return openChannelTo(recipient, defaultChannel());
        }

        OrderingFuture<NettySender> openChannelTo(ConnectionManagerWrapper recipient, ChannelInfo type) {
            return connectionManager.channel(recipient.connectionManager.consistentId(), type, recipient.connectionManager.localAddress());
        }
    }
}
