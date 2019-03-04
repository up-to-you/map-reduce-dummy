package ru.bmstu;

import ru.bmstu.Splitter.Range;

import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toList;

public final class MapReduceRunnerFactory {

    private MapReduceRunnerFactory() {}

    public static <I, K, V, O> MapReduceRunner<I, K, V, O> prepareRunner(
            Splitter<I> splitter,
            Supplier<Mapper<I, K, V>> mapperSupplier,
            Supplier<Reducer<K, V, O>> reducerSupplier) {

        return new MapReduceRunner<>(splitter, mapperSupplier, reducerSupplier);
    }

    private static class MapReduceRunner<I, K, V, O> {
        private int splitRatio;

        private I input;
        private Splitter<I> splitter;
        private Supplier<Mapper<I, K, V>> mapperSupp;
        private Supplier<Reducer<K, V, O>> reducerSupp;

        private MapReduceRunner(Splitter<I> splitter,
                                Supplier<Mapper<I, K, V>> mapperSupp,
                                Supplier<Reducer<K, V, O>> reducerSupp) {

            this.splitter = splitter;
            this.mapperSupp = mapperSupp;
            this.reducerSupp = reducerSupp;
        }

        public MapReduceRunner<I, K, V, O> input(I input) {
            this.input = input;
            return this;
        }

        public MapReduceRunner<I, K, V, O> splitRatio(int splitRatio) {
            this.splitRatio = splitRatio;
            return this;
        }

        public O run() {
            List<Range> splitRs = splitter.split(input);

//            List<Mapper<MI, K, V>> mappers = prepareWorkers(mappersSupp);
//            List<Reducer<RI, O>> reducers = prepareWorkers(reducersSupp);

//            mappers.get(0).map(splitRs);

            return null;
        }

        private <T> List<T> prepareWorkers(Supplier<T> workerSupplier) {
            return Stream.generate(workerSupplier)
                    .limit(splitRatio)
                    .collect(toList());
        }
    }
}
