package org.apache.ignite.compute.v2;

public interface Executor<T> {
    T call(Object... args);
}
