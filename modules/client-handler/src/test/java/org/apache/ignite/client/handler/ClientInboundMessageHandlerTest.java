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

package org.apache.ignite.client.handler;

import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.awaitility.Awaitility.await;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.verify;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.buffer.UnpooledByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import java.io.IOException;
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.ignite.client.handler.configuration.ClientConnectorConfiguration;
import org.apache.ignite.compute.IgniteCompute;
import org.apache.ignite.internal.catalog.CatalogService;
import org.apache.ignite.internal.configuration.testframework.ConfigurationExtension;
import org.apache.ignite.internal.configuration.testframework.InjectConfiguration;
import org.apache.ignite.internal.hlc.HybridClock;
import org.apache.ignite.internal.security.authentication.AuthenticationManager;
import org.apache.ignite.internal.security.authentication.AuthenticationManagerImpl;
import org.apache.ignite.internal.security.authentication.basic.BasicAuthenticationProviderChange;
import org.apache.ignite.internal.security.configuration.SecurityConfiguration;
import org.apache.ignite.internal.sql.engine.QueryProcessor;
import org.apache.ignite.internal.table.IgniteTablesInternal;
import org.apache.ignite.internal.table.distributed.schema.SchemaSyncService;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.internal.tx.impl.IgniteTransactionsImpl;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.network.ClusterNodeImpl;
import org.apache.ignite.network.ClusterService;
import org.apache.ignite.network.NetworkAddress;
import org.apache.ignite.network.TopologyService;
import org.apache.ignite.sql.IgniteSql;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.msgpack.core.MessagePack;

@ExtendWith(MockitoExtension.class)
@ExtendWith(ConfigurationExtension.class)
class ClientInboundMessageHandlerTest extends BaseIgniteAbstractTest {
    private static final Duration TIMEOUT_OF_DURING = Duration.ofSeconds(2);

    private static final String PROVIDER_NAME = "basic";

    @InjectConfiguration
    private ClientConnectorConfiguration configuration;

    @InjectConfiguration
    private SecurityConfiguration securityConfiguration;

    @Mock
    private IgniteTablesInternal igniteTables;

    @Mock
    private IgniteTransactionsImpl igniteTransactions;

    @Mock
    private QueryProcessor processor;

    @Mock
    private IgniteCompute compute;

    @Mock
    private TopologyService topologyService;

    @Mock
    private ClusterService clusterService;

    @Mock
    private IgniteSql sql;

    @Mock
    private CompletableFuture<UUID> clusterId;

    @Mock
    private ClientHandlerMetricSource metrics;

    @Mock
    private HybridClock clock;

    @Mock
    private SchemaSyncService schemaSyncService;

    @Mock
    private CatalogService catalogService;

    @Mock
    private ChannelHandlerContext ctx;

    @Mock
    private Channel channel;

    @Mock
    private ChannelFuture channelFuture;

    private ClientInboundMessageHandler handler;

    private final AtomicBoolean ctxClosed = new AtomicBoolean(false);

    @BeforeEach
    void setUp() throws Exception {
        doReturn(topologyService).when(clusterService).topologyService();

        ClusterNode node = new ClusterNodeImpl("node1", "node1", new NetworkAddress("localhost", 10800));
        doReturn(node).when(topologyService).localMember();

        doReturn(UUID.randomUUID()).when(clusterId).join();

        doReturn(channelFuture).when(channel).closeFuture();

        doReturn(new UnpooledByteBufAllocator(true)).when(ctx).alloc();
        doReturn(channel).when(ctx).channel();
        lenient().doAnswer(invocation -> {
            ctxClosed.set(true);
            return null;
        }).when(ctx).close();

        AuthenticationManager authenticationManager = new AuthenticationManagerImpl();
        AtomicLong clientIdGen = new AtomicLong(0);

        handler = new ClientInboundMessageHandler(
                igniteTables,
                igniteTransactions,
                processor,
                configuration.value(),
                compute,
                clusterService,
                sql,
                clusterId,
                metrics,
                authenticationManager,
                clock,
                schemaSyncService,
                catalogService,
                clientIdGen.incrementAndGet(),
                new ClientPrimaryReplicaTracker(null, catalogService, clock, schemaSyncService)
        );

        authenticationManager.listen(handler);
        securityConfiguration.listen(authenticationManager);

        CompletableFuture<Void> future = securityConfiguration.change(change -> {
            change.changeEnabled(true);
            change.changeAuthentication(authChange -> {
                authChange.changeProviders(providersChange -> {
                    providersChange.create(PROVIDER_NAME, basicChange -> {
                        basicChange.convert(BasicAuthenticationProviderChange.class)
                                .changeUsers(users ->
                                        users.create("admin", user -> user.changePassword("password"))
                                                .create("admin1", user -> user.changePassword("password")));
                    });
                });
            });
        });
        assertThat(future, willCompleteSuccessfully());

        handler.channelRegistered(ctx);
    }

    @Test
    void disableAuthentication() throws IOException {
        handshake();

        CompletableFuture<Void> future = securityConfiguration.change(change -> {
            change.changeEnabled(false);
        });
        assertThat(future, willCompleteSuccessfully());

        await().during(TIMEOUT_OF_DURING).untilAtomic(ctxClosed, is(false));
    }

    @Test
    void enableAuthentication() throws IOException {
        CompletableFuture<Void> future1 = securityConfiguration.change(change -> {
            change.changeEnabled(false);
        });
        assertThat(future1, willCompleteSuccessfully());

        handshake();

        CompletableFuture<Void> future = securityConfiguration.change(change -> {
            change.changeEnabled(true);
        });
        assertThat(future, willCompleteSuccessfully());

        await().untilAtomic(ctxClosed, is(true));
    }

    @Test
    void changeCurrentProvider() throws IOException {
        handshake();

        CompletableFuture<Void> future = securityConfiguration.change(change -> {
            change.changeEnabled(true);
            change.changeAuthentication(authChange -> {
                authChange.changeProviders(providersChange -> {
                    providersChange.update(PROVIDER_NAME, basicChange -> {
                        basicChange.convert(BasicAuthenticationProviderChange.class)
                                .changeUsers(users ->
                                        users.update("admin", user -> user.changePassword("new-password"))
                                );
                    });
                });
            });
        });
        assertThat(future, willCompleteSuccessfully());

        await().untilAtomic(ctxClosed, is(true));
    }

    @Test
    void changeAnotherProvider() throws IOException {
        handshake();

        CompletableFuture<Void> future = securityConfiguration.change(change -> {
            change.changeEnabled(true);
            change.changeAuthentication(authChange -> {
                authChange.changeProviders(providersChange -> {
                    providersChange.update(PROVIDER_NAME, basicChange -> {
                        basicChange.convert(BasicAuthenticationProviderChange.class)
                                .changeUsers(users ->
                                        users.update("admin1", user -> user.changePassword("new-password"))
                                );
                    });
                });
            });
        });

        assertThat(future, willCompleteSuccessfully());

        await().during(TIMEOUT_OF_DURING).untilAtomic(ctxClosed, is(false));
    }

    @Test
    void deleteAnotherProvider() throws IOException {
        handshake();

        CompletableFuture<Void> future = securityConfiguration.change(change -> {
            change.changeEnabled(true);
            change.changeAuthentication(authChange -> {
                authChange.changeProviders(providersChange -> {
                    providersChange.delete("basic1");
                });
            });
        });
        assertThat(future, willCompleteSuccessfully());

        await().during(TIMEOUT_OF_DURING).untilAtomic(ctxClosed, is(false));
    }

    @Test
    void createNewProvider() throws IOException {
        String newProviderName = "basic2";
        handshake();

        CompletableFuture<Void> future = securityConfiguration.change(change -> {
            change.changeEnabled(true);
            change.changeAuthentication(authChange -> {
                authChange.changeProviders(providersChange -> {
                    providersChange.create(newProviderName, basicChange -> {
                        basicChange.convert(BasicAuthenticationProviderChange.class)
                                .changeUsers(users ->
                                        users.create("admin2", user -> user.changePassword("admin"))
                                );
                    });
                });
            });
        });
        assertThat(future, willCompleteSuccessfully());

        await().during(TIMEOUT_OF_DURING).untilAtomic(ctxClosed, is(false));
    }

    private void handshake() throws IOException {
        var packer = MessagePack.newDefaultBufferPacker();
        packer.packInt(3); // Major.
        packer.packInt(0); // Minor.
        packer.packInt(0); // Patch.

        packer.packInt(2); // Client type: general purpose.

        packer.packBinaryHeader(0); // Features.
        packer.packInt(3); // Extensions.
        packer.packString("authn-type");
        packer.packString(PROVIDER_NAME);
        packer.packString("authn-identity");
        packer.packString("admin");
        packer.packString("authn-secret");
        packer.packString("password");

        ByteBuf byteBuf = Unpooled.wrappedBuffer(packer.toByteArray());

        handler.channelRead(ctx, byteBuf);

        verify(ctx).writeAndFlush(any());
    }
}
