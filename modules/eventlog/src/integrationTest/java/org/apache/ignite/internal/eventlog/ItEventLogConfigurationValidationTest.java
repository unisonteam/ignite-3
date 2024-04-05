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

package org.apache.ignite.internal.eventlog;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.apache.ignite.internal.ClusterPerClassIntegrationTest;
import org.apache.ignite.internal.eventlog.api.IgniteEventType;
import org.apache.ignite.internal.eventlog.config.schema.EventLogConfiguration;
import org.junit.jupiter.api.Test;

class ItEventLogConfigurationValidationTest extends ClusterPerClassIntegrationTest {

    @Test
    void channelShouldDefineValidEventTypes() {
        // Expect invalid event type should be validated.
        assertThrows(Exception.class, () ->
                CLUSTER.aliveNode().clusterConfiguration().getConfiguration(EventLogConfiguration.KEY)
                        .change(c -> c.changeChannels().create("testChannel", channelChange -> {
                            channelChange.changeEvents("NO_SUCH_EVENT_TYPE");
                        })).get()
        );

        // But valid event type is ok.
        assertDoesNotThrow(
                () -> CLUSTER.aliveNode().clusterConfiguration().getConfiguration(EventLogConfiguration.KEY)
                        .change(c -> c.changeChannels(cs -> cs.create("testChannel2", channelChange -> {
                            channelChange.changeEvents(IgniteEventType.CONNECTION_CLOSED.name());
                        }))).get()
        );
    }
}
