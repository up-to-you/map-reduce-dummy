package ru.bmstu.core;

import ru.bmstu.core.Splitter.Range;

import java.util.Map;

@FunctionalInterface
public interface Mapper<I, K, V> {
    Map<K, V> map(I input, Range range);
}
