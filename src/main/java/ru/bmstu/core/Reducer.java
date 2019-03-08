package ru.bmstu.core;

@FunctionalInterface
public interface Reducer<D, O> {
    O reduce(D input);
}
