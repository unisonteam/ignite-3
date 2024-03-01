package org.apache.ignite.internal.eventlog.channel;

import org.apache.ignite.internal.eventlog.Event;

public class Channel {
    private final String name;

    public Channel(String name) {
        this.name = name;
    }

    public void write(Event event) {
    }
}
