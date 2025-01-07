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

package org.apache.ignite.internal.eventlog.impl;

import static java.util.concurrent.CompletableFuture.completedFuture;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import org.apache.ignite.configuration.NamedListView;
import org.apache.ignite.configuration.notifications.ConfigurationListener;
import org.apache.ignite.configuration.notifications.ConfigurationNotificationEvent;
import org.apache.ignite.internal.eventlog.api.EventChannel;
import org.apache.ignite.internal.eventlog.config.schema.ChannelView;
import org.apache.ignite.internal.eventlog.config.schema.EventLogConfiguration;

public class ConfigurationBasedChannelRegistry implements ChannelRegistry {
    private final AtomicReference<Map<String, EventChannel>> cache;

    private final AtomicReference<Map<String, Set<EventChannel>>> typeCache;

    private final SinkRegistry sinkRegistry;

    public ConfigurationBasedChannelRegistry(EventLogConfiguration cfg, SinkRegistry sinkRegistry) {
        this.cache = new AtomicReference<>(new HashMap<>());
        this.typeCache = new AtomicReference<>(new HashMap<>());
        this.sinkRegistry = sinkRegistry;

        cfg.channels().listen(new CacheUpdater());
    }

    @Override
    public EventChannel getByName(String name) {
        return cache.get().get(name);
    }

    @Override
    public Set<EventChannel> findAllChannelsByEventType(String igniteEventType) {
        Set<EventChannel> result = typeCache.get().get(igniteEventType);
        return result == null ? Set.of() : new HashSet<>(result);
    }

    private class CacheUpdater implements ConfigurationListener<NamedListView<ChannelView>> {
        @Override
        public CompletableFuture<?> onUpdate(ConfigurationNotificationEvent<NamedListView<ChannelView>> ctx) {
            NamedListView<ChannelView> newListValue = ctx.newValue();

            Map<String, EventChannel> newCache = new HashMap<>();
            Map<String, Set<EventChannel>> newTypeCache = new HashMap<>();

            newListValue.forEach(view -> {
                if (view.enabled()) {
                    EventChannel channel = createChannel(view);
                    newCache.put(view.name(), channel);
                    for (String eventType : view.events()) {
                        newTypeCache.computeIfAbsent(
                                eventType.trim(),
                                t -> new HashSet<>()
                        ).add(channel);
                    }
                }
            });

            cache.set(newCache);
            typeCache.set(newTypeCache);

            return completedFuture(null);
        }

        private EventChannel createChannel(ChannelView view) {
            Set<String> types = Arrays.stream(view.events())
                    .map(String::trim)
                    .collect(Collectors.toSet());

            return new EventChannelImpl(view.name(), types, sinkRegistry);
        }
    }
}
