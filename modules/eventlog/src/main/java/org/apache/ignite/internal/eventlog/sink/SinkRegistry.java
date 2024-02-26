package org.apache.ignite.internal.eventlog.sink;

import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.configuration.NamedConfigurationTree;
import org.apache.ignite.configuration.notifications.ConfigurationNamedListListener;
import org.apache.ignite.configuration.notifications.ConfigurationNotificationEvent;
import org.apache.ignite.internal.eventlog.configuration.sink.FileSinkView;
import org.apache.ignite.internal.eventlog.configuration.sink.SinkChange;
import org.apache.ignite.internal.eventlog.configuration.sink.SinkConfiguration;
import org.apache.ignite.internal.eventlog.configuration.sink.SinkView;
import org.apache.ignite.internal.eventlog.sink.file.FileSink;

public class SinkRegistry {
    private final Path workDir;
    private final Map<String, Sink> sinks = new HashMap<>();
    private final Map<String, List<String>> channelsBySink = new HashMap<>();
    private final Map<String, List<Sink>> sinksByChannel = new HashMap<>();

    public SinkRegistry(NamedConfigurationTree<SinkConfiguration, SinkView, SinkChange> sinksConfiguration, Path workDir) {
        this.workDir = workDir;
        sinksConfiguration.listenElements(new SinkConfigurationUpdateListener());
    }

    public Collection<Sink> allSinks() {
       return sinks.values();
    }

    public List<Sink> findByChannel(String channelName) {
        return sinksByChannel.get(channelName);
    }

    private class SinkConfigurationUpdateListener implements ConfigurationNamedListListener<SinkView> {
        @Override
        public CompletableFuture<?> onCreate(ConfigurationNotificationEvent<SinkView> ctx) {
            if (Objects.requireNonNull(ctx.newValue()).type().equals("file")) {
                FileSinkView fileSinkView = (FileSinkView) ctx.newValue();
                Sink fileSink = new FileSink(workDir, fileSinkView.pattern(), fileSinkView.name());
                sinks.put(fileSinkView.name(), fileSink);
                Arrays.stream(fileSinkView.channels())
                        .forEach(channel -> sinksByChannel.merge(channel, List.of(fileSink), (l1, l2) -> {
                             List<Sink> merged = new ArrayList<>(l1);
                             merged.addAll(l2);
                             return merged;
                        }));
                channelsBySink.put(fileSinkView.name(), List.of(fileSinkView.channels()));
            }

            return nullCompletedFuture();
        }

        @Override
        public CompletableFuture<?> onUpdate(ConfigurationNotificationEvent<SinkView> ctx) {
            if (ctx.newValue().type().equals("file")) {
                FileSinkView fileSinkView = (FileSinkView) ctx.newValue();
                Sink fileSink = new FileSink(workDir, fileSinkView.pattern(), fileSinkView.name());
                sinks.put(fileSinkView.name(), fileSink);
                Arrays.stream(fileSinkView.channels())
                        .forEach(channel -> sinksByChannel.merge(channel, List.of(fileSink), (l1, l2) -> {
                            List<Sink> merged = new ArrayList<>(l1);
                            merged.addAll(l2);
                            return merged;
                        }));
                channelsBySink.put(fileSinkView.name(), List.of(fileSinkView.channels()));
            }

            return nullCompletedFuture();
        }

        @Override
        public CompletableFuture<?> onDelete(ConfigurationNotificationEvent<SinkView> ctx) {
            Sink sink = sinks.remove(ctx.oldValue().name());
            Arrays.stream(ctx.oldValue().channels())
                    .forEach(channel -> sinksByChannel.get(channel).remove(sink));
            channelsBySink.remove(ctx.oldValue().name());
            return nullCompletedFuture();
        }
    }
}
