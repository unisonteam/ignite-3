package org.apache.ignite.internal.eventlog;

import java.util.function.Supplier;

public class EventLog {

    private final EventRouter router;

    public EventLog(EventRouter router) {
        this.router = router;
    }

    void start() {
    }

    public void fire(EventDescriptor descriptor) {
        router.routeByType(descriptor.type()).forEach(channel -> channel.write(descriptor.eventSupplier().get()));
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
