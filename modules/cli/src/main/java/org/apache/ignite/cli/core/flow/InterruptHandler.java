package org.apache.ignite.cli.core.flow;

public interface InterruptHandler<T> {
    void handle(Flowable<T> value);
}
