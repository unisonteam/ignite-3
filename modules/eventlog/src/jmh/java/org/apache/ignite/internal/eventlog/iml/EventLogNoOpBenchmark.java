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

package org.apache.ignite.internal.eventlog.iml;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Stream;
import org.apache.ignite.configuration.ConfigurationProperty;
import org.apache.ignite.configuration.NamedConfigurationTree;
import org.apache.ignite.configuration.NamedListChange;
import org.apache.ignite.configuration.NamedListView;
import org.apache.ignite.configuration.notifications.ConfigurationListener;
import org.apache.ignite.configuration.notifications.ConfigurationNamedListListener;
import org.apache.ignite.configuration.notifications.ConfigurationNotificationEvent;
import org.apache.ignite.internal.eventlog.api.Event;
import org.apache.ignite.internal.eventlog.api.EventChannel;
import org.apache.ignite.internal.eventlog.api.EventLog;
import org.apache.ignite.internal.eventlog.api.IgniteEvents;
import org.apache.ignite.internal.eventlog.config.schema.ChannelChange;
import org.apache.ignite.internal.eventlog.config.schema.ChannelConfiguration;
import org.apache.ignite.internal.eventlog.config.schema.ChannelView;
import org.apache.ignite.internal.eventlog.config.schema.EventLogChange;
import org.apache.ignite.internal.eventlog.config.schema.EventLogView;
import org.apache.ignite.internal.eventlog.config.schema.LogSinkView;
import org.apache.ignite.internal.eventlog.config.schema.SinkChange;
import org.apache.ignite.internal.eventlog.config.schema.SinkConfiguration;
import org.apache.ignite.internal.eventlog.config.schema.SinkView;
import org.apache.ignite.internal.eventlog.event.EventUser;
import org.apache.ignite.internal.eventlog.impl.ChannelRegistry;
import org.apache.ignite.internal.eventlog.impl.ConfigurationBasedChannelRegistry;
import org.apache.ignite.internal.eventlog.impl.ConfigurationBasedSinkRegistry;
import org.apache.ignite.internal.eventlog.impl.EventLogImpl;
import org.apache.ignite.internal.eventlog.impl.LogSinkFactory;
import org.apache.ignite.internal.eventlog.ser.EventSerializer;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.infra.Blackhole;

@State(Scope.Benchmark)
public class EventLogNoOpBenchmark {

    private EventLog noOpBaselineEventlog;

    private EventLog eventLog;

    @Setup
    public void setup() {
        noOpBaselineEventlog = new NoOpEventLog(true);

        EventSerializer eventSerializer = null;

        StubEventLogConfiguration configuration = new StubEventLogConfiguration(
                new StubNamedConfigurationTree<>(), new StubNamedConfigurationTree<>()
        );

        var sinkFactory = new LogSinkFactory(eventSerializer);
        var sinkRegistry = new ConfigurationBasedSinkRegistry(configuration, sinkFactory);
        var channelRegistry = new ConfigurationBasedChannelRegistry(configuration, sinkRegistry);

        eventLog = new EventLogImpl(channelRegistry);

        configuration.sinks.listeners.forEach(l -> l.onUpdate(
                new StubConfigurationNotificationEvent(
                        new StubNamedListView<>(List.of(new StubSinkView("SinkName", "log", "SinkChannel")))
                )
        ));
        configuration.channels.listeners.forEach(l -> l.onUpdate(
                new StubConfigurationNotificationEvent(
                        new StubNamedListView<>(
                                List.of(new StubChannelView("ChannelName", false, new String[]{"USER_AUTHENTICATION_SUCCESS"})))
                )
        ));
    }

    @Benchmark
    public void minimalBaseline(Blackhole bh) {
        noOpBaselineEventlog.log(() -> IgniteEvents.CLIENT_CONNECTION_CLOSED.create(EventUser.system()));
        bh.consume(1);
    }

    @Benchmark
    public void eventLogImpl(Blackhole bh) {
        eventLog.log(() -> IgniteEvents.CLIENT_CONNECTION_ESTABLISHED.create(EventUser.system()));
        bh.consume(1);
    }

    private static class TestChannelRegistry implements ChannelRegistry {
        private final Map<String, Supplier<EventChannel>> channels;

        private TestChannelRegistry() {
            channels = new HashMap<>();
        }

        void register(String name, Supplier<EventChannel> channel) {
            channels.put(name, channel);
        }

        @Override
        public EventChannel getByName(String name) {
            return channels.get(name).get();
        }

        @Override
        public Set<EventChannel> findAllChannelsByEventType(String igniteEventType) {
            return channels.values().stream()
                    .map(Supplier::get)
                    .filter(channel -> channel.types().contains(igniteEventType))
                    .collect(HashSet::new, Set::add, Set::addAll);
        }
    }

    private static class NoOpEventLog implements EventLog {
        private final boolean isEnabled;

        private NoOpEventLog(boolean isEnabled) {
            this.isEnabled = isEnabled;
        }

        @Override
        public void log(Supplier<Event> eventProvider) {
            if (isEnabled) {
                eventProvider.get();
            } else {
                // No-op.
            }
        }
    }

    private static class StubEventLogView implements EventLogView {
        private final List<SinkView> sinks;
        private final List<ChannelView> channels;

        private StubEventLogView(List<SinkView> sinks, List<ChannelView> channels) {
            this.sinks = sinks;
            this.channels = channels;
        }

        @Override
        public NamedListView<? extends SinkView> sinks() {
            return new StubNamedListView<>(sinks);
        }

        @Override
        public NamedListView<? extends ChannelView> channels() {
            return new StubNamedListView<>(channels);
        }
    }

    private static class StubChannelView implements ChannelView {
        private final String name;
        private final boolean enabled;
        private final String[] events;

        private StubChannelView(String name, boolean enabled, String[] events) {
            this.name = name;
            this.enabled = enabled;
            this.events = events;
        }

        @Override
        public String name() {
            return name;
        }

        @Override
        public String[] events() {
            return events;
        }

        @Override
        public boolean enabled() {
            return enabled;
        }
    }

    private static class StubSinkView implements LogSinkView  {
        private final String name;
        private final String type;
        private final String channel;

        private StubSinkView(String name, String type, String channel) {
            this.name = name;
            this.type = type;
            this.channel = channel;
        }

        @Override
        public String type() {
            return type;
        }

        @Override
        public String name() {
            return name;
        }

        @Override
        public String channel() {
            return channel;
        }

        @Override
        public String criteria() {
            return "";
        }

        @Override
        public String level() {
            return "";
        }

        @Override
        public String format() {
            return "";
        }
    }


    private static class StubConfigurationNotificationEvent<V> implements ConfigurationNotificationEvent<V> {
        private final V v;

        private StubConfigurationNotificationEvent(V v) {
            this.v = v;
        }

        @Override
        public V oldValue() {
            return null;
        }

        @Override
        public <T> T oldValue(Class<T> viewClass) {
            return null;
        }

        @Override
        public V newValue() {
            return v;
        }

        @Override
        public <T> T newValue(Class<T> viewClass) {
            return null;
        }

        @Override
        public long storageRevision() {
            return 0;
        }

        @Override
        public String oldName(Class<?> viewClass) {
            return "";
        }

        @Override
        public String newName(Class<?> viewClass) {
            return "";
        }
    }


    private static class StubNamedConfigurationTree<T extends ConfigurationProperty<V>, V, C extends V>
            implements NamedConfigurationTree<T, V, C> {

        List<ConfigurationListener<NamedListView<V>>> listeners = new ArrayList<>();

        @Override
        public T get(String name) {
            return null;
        }

        @Override
        public T get(UUID internalId) {
            return null;
        }

        @Override
        public List<UUID> internalIds() {
            return List.of();
        }

        @Override
        public void listenElements(ConfigurationNamedListListener<V> listener) {

        }

        @Override
        public void stopListenElements(ConfigurationNamedListListener<V> listener) {

        }

        @Override
        public T any() {
            return null;
        }

        @Override
        public NamedConfigurationTree<T, V, C> directProxy() {
            return null;
        }

        @Override
        public CompletableFuture<Void> change(Consumer<NamedListChange<V, C>> change) {
            return null;
        }

        @Override
        public String key() {
            return "";
        }

        @Override
        public NamedListView<V> value() {
            return null;
        }

        @Override
        public void listen(ConfigurationListener<NamedListView<V>> listener) {
            listeners.add(listener);
        }

        @Override
        public void stopListen(ConfigurationListener<NamedListView<V>> listener) {

        }
    }


    private static class StubEventLogConfiguration implements org.apache.ignite.internal.eventlog.config.schema.EventLogConfiguration {

        private final StubNamedConfigurationTree<SinkConfiguration, SinkView, SinkChange> sinks;
        private final StubNamedConfigurationTree<ChannelConfiguration, ChannelView, ChannelChange> channels;

        private StubEventLogConfiguration(StubNamedConfigurationTree<SinkConfiguration, SinkView, SinkChange> sinks,
                StubNamedConfigurationTree<ChannelConfiguration, ChannelView, ChannelChange> channels) {
            this.sinks = sinks;
            this.channels = channels;
        }


        @Override
        public NamedConfigurationTree<SinkConfiguration, SinkView, SinkChange> sinks() {
            return sinks;
        }

        @Override
        public NamedConfigurationTree<ChannelConfiguration, ChannelView, ChannelChange> channels() {
            return channels;
        }

        @Override
        public org.apache.ignite.internal.eventlog.config.schema.EventLogConfiguration directProxy() {
            return null;
        }

        @Override
        public CompletableFuture<Void> change(Consumer<EventLogChange> change) {
            return null;
        }

        @Override
        public String key() {
            return "";
        }

        @Override
        public EventLogView value() {
            return null;
        }

        @Override
        public void listen(ConfigurationListener<EventLogView> listener) {
        }

        @Override
        public void stopListen(ConfigurationListener<EventLogView> listener) {

        }
    }

    public static class StubNamedListView<T> implements NamedListView<T> {
        private final List<T> values;

        public StubNamedListView(List<T> values) {
            this.values = values;
        }

        @Override
        public List<String> namedListKeys() {
            return List.of();
        }

        @Override
        public T get(String key) {
            return null;
        }

        @Override
        public T get(UUID internalId) {
            return null;
        }

        @Override
        public T get(int index) throws IndexOutOfBoundsException {
            return null;
        }

        @Override
        public int size() {
            return 0;
        }

        @Override
        public Stream<T> stream() {
            return values.stream();
        }
    }
}
