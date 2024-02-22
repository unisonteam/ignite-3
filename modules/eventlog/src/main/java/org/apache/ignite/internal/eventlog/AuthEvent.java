package org.apache.ignite.internal.eventlog;

public class AuthEvent implements Event {
    private final String username;

    private AuthEvent(String username) {
        this.username = username;
    }

    @Override
    public String toString() {
        return "AuthEvent{" +
                "username='" + username + '\'' +
                '}';
    }

    @Override
    public EventType type() {
        return EventType.AUTHENTICATION;
    }
}
