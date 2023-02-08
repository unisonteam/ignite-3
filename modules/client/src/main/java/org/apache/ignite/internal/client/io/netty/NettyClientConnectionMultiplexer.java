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

package org.apache.ignite.internal.client.io.netty;

import static org.apache.ignite.lang.ErrorGroups.Common.UNKNOWN_ERR;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.ssl.ClientAuth;
import io.netty.handler.ssl.SslContextBuilder;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateException;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.TrustManagerFactory;
import org.apache.ignite.client.ClientAuthConfiguration;
import org.apache.ignite.client.IgniteClientConfiguration;
import org.apache.ignite.client.IgniteClientConnectionException;
import org.apache.ignite.client.IgniteClientSslException;
import org.apache.ignite.internal.client.io.ClientConnection;
import org.apache.ignite.internal.client.io.ClientConnectionMultiplexer;
import org.apache.ignite.internal.client.io.ClientConnectionStateHandler;
import org.apache.ignite.internal.client.io.ClientMessageHandler;
import org.apache.ignite.internal.client.proto.ClientMessageDecoder;

/**
 * Netty-based multiplexer.
 */
public class NettyClientConnectionMultiplexer implements ClientConnectionMultiplexer {
    private final NioEventLoopGroup workerGroup;

    private final Bootstrap bootstrap;

    /**
     * Constructor.
     */
    public NettyClientConnectionMultiplexer() {
        workerGroup = new NioEventLoopGroup();
        bootstrap = new Bootstrap();
    }

    /** {@inheritDoc} */
    @Override
    public void start(IgniteClientConfiguration clientCfg) {
        try {
            bootstrap.group(workerGroup);
            bootstrap.channel(NioSocketChannel.class);
            bootstrap.option(ChannelOption.SO_KEEPALIVE, true);
            bootstrap.option(ChannelOption.CONNECT_TIMEOUT_MILLIS, (int) clientCfg.connectTimeout());
            bootstrap.handler(new ChannelInitializer<SocketChannel>() {
                @Override
                public void initChannel(SocketChannel ch) {
                    setupSsl(ch, clientCfg);
                    ch.pipeline().addLast(
                            new ClientMessageDecoder(),
                            new NettyClientMessageHandler());
                }
            });

        } catch (Throwable t) {
            workerGroup.shutdownGracefully();

            throw t;
        }
    }

    private void setupSsl(SocketChannel ch, IgniteClientConfiguration clientCfg) {
        if (clientCfg.sslConfiguration() == null || !clientCfg.sslConfiguration().enabled()) {
            return;
        }

        try {
            var trustStore = clientCfg.sslConfiguration().trustStore();
            if (trustStore == null) {
                throw new IgniteClientSslException("SSL is enabled but no trust store is provided");
            }
            if (trustStore.path() == null) {
                throw new IgniteClientSslException("SSL is enabled but no path to trust store is provided");
            }

            KeyStore ts = KeyStore.getInstance(trustStore.type());
            ts.load(
                    Files.newInputStream(Path.of(trustStore.path())),
                    trustStore.password() == null
                            ? null
                            : trustStore.password().toCharArray()
            );

            TrustManagerFactory trustManagerFactory = TrustManagerFactory.getInstance(
                    TrustManagerFactory.getDefaultAlgorithm()
            );
            trustManagerFactory.init(ts);

            var builder = SslContextBuilder.forClient().trustManager(trustManagerFactory);

            ClientAuth clientAuth = toNettyClientAuth(clientCfg.sslConfiguration().clientAuth());
            if (ClientAuth.NONE != clientAuth) {
                var keystore = clientCfg.sslConfiguration().keyStore();
                if (keystore == null) {
                    throw new IgniteClientSslException("SSL client authentication is enabled but no key store is provided");
                }
                if (keystore.path() == null) {
                    throw new IgniteClientSslException("SSL client authentication is enabled but no key store path is provided");
                }

                char[] ksPassword = keystore.password() == null ? null : keystore.password().toCharArray();

                KeyStore ks = KeyStore.getInstance(keystore.type());
                ks.load(Files.newInputStream(Path.of(keystore.path())), ksPassword);

                KeyManagerFactory keyManagerFactory = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
                keyManagerFactory.init(ks, ksPassword);

                builder.clientAuth(clientAuth).keyManager(keyManagerFactory);
            }

            var context = builder.build();

            ch.pipeline().addFirst("ssl", context.newHandler(ch.alloc()));
        } catch (NoSuchAlgorithmException | KeyStoreException | CertificateException | IOException | UnrecoverableKeyException e) {
            throw new IgniteClientSslException("Client SSL configuration error: " + e.getMessage(), e);
        }

    }

    private ClientAuth toNettyClientAuth(ClientAuthConfiguration igniteClientAuth) {
        switch (igniteClientAuth) {
            case NONE: return ClientAuth.NONE;
            case REQUIRE: return ClientAuth.REQUIRE;
            case OPTIONAL: return ClientAuth.OPTIONAL;
            default: throw new IllegalArgumentException("Client authentication type is not supported");
        }
    }

    /** {@inheritDoc} */
    @Override
    public void stop() {
        workerGroup.shutdownGracefully();
    }

    /** {@inheritDoc} */
    @Override
    public ClientConnection open(InetSocketAddress addr,
            ClientMessageHandler msgHnd,
            ClientConnectionStateHandler stateHnd)
            throws IgniteClientConnectionException {
        try {
            // TODO: Async startup IGNITE-15357.
            ChannelFuture f = bootstrap.connect(addr).syncUninterruptibly();

            return new NettyClientConnection(f.channel(), msgHnd, stateHnd);
        } catch (Throwable t) {
            throw new IgniteClientConnectionException(UNKNOWN_ERR, t.getMessage(), t);
        }
    }
}
