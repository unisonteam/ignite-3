package org.apache.ignite.internal.eventlog;

public class AuthEvent implements Event {
    private final String username;
    private final boolean success;


    public AuthEvent(String username, boolean success) {
        this.username = username;
        this.success = success;
    }

    @Override
    public String toString() {
        return "AuthEvent{" +
                "username='" + username + '\'' +
                ", success=" + success +
                '}';
    }

    @Override
    public EventType type() {
        return EventType.AUTHENTICATION;
    }
}
