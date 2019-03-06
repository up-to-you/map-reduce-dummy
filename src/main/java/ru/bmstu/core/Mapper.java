package ru.bmstu.core;

import java.nio.file.Path;
import java.util.Map;

@FunctionalInterface
public interface Mapper<K, V> {
    Map<K, V> map(Path input);
}
