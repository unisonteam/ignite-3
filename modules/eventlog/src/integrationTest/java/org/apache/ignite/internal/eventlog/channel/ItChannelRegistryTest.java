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

package org.apache.ignite.internal.eventlog.channel;

import static org.awaitility.Awaitility.await;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.emptyCollectionOf;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.notNullValue;

import org.apache.ignite.internal.ClusterPerClassIntegrationTest;
import org.apache.ignite.internal.app.IgniteImpl;
import org.apache.ignite.internal.eventlog.configuration.EventLogConfiguration;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

public class ItChannelRegistryTest extends ClusterPerClassIntegrationTest {
    private static final String TEST_CHANNEL_NAME = "test-channel";

    EventLogConfiguration eventLogConfiguration;

    ChannelRegistry channelRegistry;

    @BeforeEach
    void beforeEach(TestInfo testInfo) {
        IgniteImpl ignite = CLUSTER.aliveNode();
        eventLogConfiguration = ignite.clusterConfiguration().getConfiguration(EventLogConfiguration.KEY);
        channelRegistry = new ChannelRegistry(eventLogConfiguration.channels());
    }

    @Override
    protected int initialNodes() {
        return 1;
    }

    @Test
    void channelConfiguration() {
        // Given no channels configured by default.
        assertThat(channelRegistry.allChannels(), emptyCollectionOf(Channel.class));

        // When adding a channel to the configuration.
        eventLogConfiguration.channels().change(
                c -> c.create(TEST_CHANNEL_NAME, channelChange -> channelChange.changeEnabled(true))
        );

        // Then the channel will be added to the registry.
        await().untilAsserted(() -> assertThat(channelRegistry.allChannels(), hasSize(1)));
        // And the channel can be found by name.
        assertThat(channelRegistry.findByName(TEST_CHANNEL_NAME), notNullValue());

        // When removing the channel from the configuration.
        eventLogConfiguration.channels().change(
                c -> c.delete(TEST_CHANNEL_NAME)
        );

        // Then the channel will be removed from the registry.
        await().untilAsserted(() -> assertThat(channelRegistry.allChannels(), emptyCollectionOf(Channel.class)));
    }
}
