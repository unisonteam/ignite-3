package org.apache.ignite.internal.eventlog.sink;

import org.apache.ignite.internal.eventlog.Event;

public interface Sink {
    void write(Event event);

    void flush();
}
