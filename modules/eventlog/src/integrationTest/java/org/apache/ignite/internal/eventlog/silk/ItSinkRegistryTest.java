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

package org.apache.ignite.internal.eventlog.silk;

import static org.awaitility.Awaitility.await;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.emptyCollectionOf;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.typeCompatibleWith;

import java.io.IOException;
import java.nio.file.Files;
import org.apache.ignite.internal.ClusterPerClassIntegrationTest;
import org.apache.ignite.internal.app.IgniteImpl;
import org.apache.ignite.internal.eventlog.configuration.EventLogConfiguration;
import org.apache.ignite.internal.eventlog.configuration.sink.FileSinkChange;
import org.apache.ignite.internal.eventlog.sink.Sink;
import org.apache.ignite.internal.eventlog.sink.SinkRegistry;
import org.apache.ignite.internal.eventlog.sink.file.FileSink;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

public class ItSinkRegistryTest extends ClusterPerClassIntegrationTest {
    private static final String TEST_SINK_NAME = "test-sink";
    private static final String TEST_CHANNEL_NAME = "test-channel";

    EventLogConfiguration eventLogConfiguration;

    SinkRegistry sinkRegistry;

    @BeforeEach
    void beforeEach(TestInfo testInfo) throws IOException {
        IgniteImpl ignite = CLUSTER.aliveNode();
        eventLogConfiguration = ignite.clusterConfiguration().getConfiguration(EventLogConfiguration.KEY);
        // fixme
        sinkRegistry = new SinkRegistry(eventLogConfiguration.sinks(), Files.createTempDirectory(""));
    }

    @Override
    protected int initialNodes() {
        return 1;
    }

    @Test
    void sinkConfiguration() {
        // Given no sinks configured by default.
        assertThat(sinkRegistry.allSinks(), emptyCollectionOf(Sink.class));

        // When adding a sink to the configuration without any channel.
        eventLogConfiguration.sinks().change(
                ch -> ch.create(
                        TEST_SINK_NAME,
                        sch -> sch.convert(FileSinkChange.class).changePattern("file-to-write.log")
                )
        );

        // Then the sink will be added to the registry.
        await().untilAsserted(() -> assertThat(sinkRegistry.allSinks(), hasSize(1)));
        // And the sink will not be associated with channel.
        assertThat(sinkRegistry.findByChannel(TEST_CHANNEL_NAME), nullValue());

        // When change the sink to be associated with a channel.
        eventLogConfiguration.sinks().change(
                ch -> ch.update(
                        TEST_SINK_NAME,
                        sch -> sch.convert(FileSinkChange.class)
                                .changePattern("changed-pattern.log")
                                .changeChannels(TEST_CHANNEL_NAME)
                )
        );

        // Then the sink will be associated with the channel.
        await().untilAsserted(() -> assertThat(sinkRegistry.findByChannel(TEST_CHANNEL_NAME), hasSize(1)));
        // And it is a FileSink.
        Sink sink = sinkRegistry.findByChannel(TEST_CHANNEL_NAME).get(0);
        assertThat(sink.getClass(), typeCompatibleWith(FileSink.class));

        // When removing the sink from the configuration.
        eventLogConfiguration.sinks().change(
                ch -> ch.delete(TEST_SINK_NAME)
        );

        // Then the sink will be removed from the registry.
        await().untilAsserted(() -> assertThat(sinkRegistry.allSinks(), emptyCollectionOf(Sink.class)));
    }
}
