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

package org.apache.ignite.internal.network.ssl;

import io.netty.handler.ssl.ClientAuth;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateException;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.TrustManagerFactory;
import org.apache.ignite.internal.network.configuration.SslView;
import org.apache.ignite.lang.ErrorGroups.Common;
import org.apache.ignite.lang.IgniteException;

/** Client ssl context provider. */
public class ClientSslContextProvider implements SslContextProvider {

    private final SslView ssl;

    ClientSslContextProvider(SslView ssl) {
        this.ssl = ssl;
    }

    @Override
    public SslContext createSslContext() {
        try {
            KeyStore store = KeyStore.getInstance(ssl.trustStore().type());
            store.load(
                    Files.newInputStream(Path.of(ssl.trustStore().path())),
                    ssl.trustStore().password() == null ? null : ssl.trustStore().password().toCharArray()
            );

            TrustManagerFactory trustManagerFactory = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
            trustManagerFactory.init(store);

            var builder = SslContextBuilder.forClient().trustManager(trustManagerFactory);
            ClientAuth clientAuth = ClientAuth.valueOf(ssl.clientAuth().toUpperCase());
            if (ClientAuth.NONE != clientAuth) {
                KeyStore keystore = KeyStore.getInstance(ssl.keyStore().type());
                keystore.load(Files.newInputStream(Path.of(ssl.keyStore().path())), ssl.keyStore().password().toCharArray());

                KeyManagerFactory keyManagerFactory = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
                keyManagerFactory.init(keystore, ssl.keyStore().password().toCharArray());

                builder.keyManager(keyManagerFactory);
            }

            return builder.build();
        } catch (NoSuchFileException e) {
            throw new IgniteException(Common.SSL_CONFIGURATION_ERR, String.format("File %s not found", ssl.trustStore().path()), e);
        } catch (IOException | CertificateException | KeyStoreException | NoSuchAlgorithmException | UnrecoverableKeyException e) {
            throw new IgniteException(Common.SSL_CONFIGURATION_ERR, e);
        }
    }
}
