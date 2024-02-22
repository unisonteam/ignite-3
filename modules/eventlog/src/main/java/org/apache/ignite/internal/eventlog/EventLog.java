package org.apache.ignite.internal.eventlog;

import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;
import org.apache.ignite.configuration.notifications.ConfigurationNamedListListener;
import org.apache.ignite.configuration.notifications.ConfigurationNotificationEvent;
import org.apache.ignite.internal.eventlog.configuration.sink.FileSinkView;
import org.apache.ignite.internal.eventlog.configuration.sink.SinkView;
import org.apache.ignite.internal.eventlog.sink.file.FileSink;

public class EventLog {

    private final ConfigurationNamedListListener<SinkView> eventLogConfigurationListener;

    private final EventRouter router;

    public EventLog(EventRouter router) {
        this.router = router;
        eventLogConfigurationListener = new ConfigurationNamedListListener<>() {
            @Override
            public CompletableFuture<?> onCreate(ConfigurationNotificationEvent<SinkView> ctx) {
                if (Objects.requireNonNull(ctx.newValue()).type().equals("file")) {
                    FileSinkView fileSinkView = (FileSinkView) ctx.newValue();
                    fileSink = new FileSink(workDir, fileSinkView.pattern(), fileSinkView.name());
                }

                return nullCompletedFuture();
            }
        };
    }

    void start() {

        eventLogConfiguration.sinks().listenElements(eventLogConfigurationListener);
    }

    public void fire(EventDescriptor descriptor) {
        channels.get(descriptor.type()).forEach(channel -> channel.write(descriptor.eventSupplier().get()));
    }

    public static class EventDescriptor {
       private final EventType type;
       private final Supplier<Event> eventSupplier;

        public EventDescriptor(EventType type, Supplier<Event> eventSupplier) {
            this.type = type;
            this.eventSupplier = eventSupplier;
        }

        public EventType type() {
            return type;
        }

        public Supplier<Event> eventSupplier() {
            return eventSupplier;
        }
    }
}
