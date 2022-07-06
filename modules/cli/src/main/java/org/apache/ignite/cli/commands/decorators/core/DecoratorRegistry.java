package org.apache.ignite.cli.commands.decorators.core;

import java.util.HashMap;
import java.util.Map;
import org.apache.ignite.cli.commands.decorators.DefaultDecorator;

public class DecoratorRegistry {
    private static final Map<Class<?>, Decorator<?, TerminalOutput>> store = new HashMap<>();
    
    public static <T> void add(Class<T> clazz, Decorator<T, TerminalOutput> decorator) {
        store.put(clazz, decorator);
    }

    @SuppressWarnings("unchecked")
    public static <T> Decorator<T, TerminalOutput> getDecorator(Class<T> clazz) {
        return (Decorator<T, TerminalOutput>) store.getOrDefault(clazz, new DefaultDecorator<T>());
    }
}
