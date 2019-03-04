package ru.bmstu.core;

import java.util.Map;

@FunctionalInterface
public interface Reducer<K, V, O> {
    O reduce(Map<K, V> input);
}