package ru.bmstu.core;

import java.nio.file.Path;
import java.util.List;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toList;

public final class MapReduceRunnerFactory {

    private MapReduceRunnerFactory() {}

    public static <I, K, V, O> MapReduceRunner<I, K, V, O> prepareRunner(
            Splitter<I> splitter,
            Mapper<K, V> mapper,
            Reducer<K, V, O> reducer) {

        return new MapReduceRunner<>(splitter, mapper, reducer);
    }

    public static class MapReduceRunner<I, K, V, O> {

        private boolean isParallel = true;
        private int splitRatio;

        private I input;
        private Splitter<I> splitter;
        private Mapper<K, V> mapper;
        private Reducer<K, V, O> reducer;

        private MapReduceRunner(Splitter<I> splitter,
                                Mapper<K, V> mapper,
                                Reducer<K, V, O> reducer) {

            this.splitter = splitter;
            this.mapper = mapper;
            this.reducer = reducer;
        }

        public MapReduceRunner<I, K, V, O> input(I input) {
            this.input = input;
            return this;
        }

        public MapReduceRunner<I, K, V, O> splitRatio(int splitRatio) {
            this.splitRatio = splitRatio;
            return this;
        }

        public MapReduceRunner<I, K, V, O> parallel(boolean isParallel) {
            this.isParallel = isParallel;
            return this;
        }

        public List<O> run() {
            List<Path> parts = splitter.split(input, splitRatio);

            Stream<Path> partsStream = isParallel ? parts.parallelStream() : parts.stream();

            return partsStream
                    .map(mapper::map)
                    .map(reducer::reduce)
                    .collect(toList());
        }
    }
}
