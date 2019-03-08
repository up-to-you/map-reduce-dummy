package ru.bmstu.core;

import java.nio.file.Path;
import java.util.Map;

@FunctionalInterface
public interface Mapper<D> {
    D map(Path input);
}
