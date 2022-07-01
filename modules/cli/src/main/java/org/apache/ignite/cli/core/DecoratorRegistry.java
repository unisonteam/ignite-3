package org.apache.ignite.cli.core;

import java.util.HashMap;
import java.util.Map;
import org.apache.ignite.cli.commands.decorators.DefaultDecorator;
import org.apache.ignite.cli.commands.decorators.core.Decorator;
import org.apache.ignite.cli.commands.decorators.core.TerminalOutput;

public class DecoratorRegistry {
    private final Map<Class<?>, Decorator<?, TerminalOutput>> store = new HashMap<>();

    public <T> void add(Class<T> clazz, Decorator<T, TerminalOutput> decorator) {
        store.put(clazz, decorator);
    }

    public <T> Decorator<T, TerminalOutput> getDecorator(Class<T> clazz) {
        return (Decorator<T, TerminalOutput>) store.getOrDefault(clazz, new DefaultDecorator<T>());
    }
}
