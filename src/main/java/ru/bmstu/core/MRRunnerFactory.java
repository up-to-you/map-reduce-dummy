package ru.bmstu.core;

import java.util.List;
import java.util.Objects;
import java.util.stream.Stream;

import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

public class MRRunnerFactory {

    private MRRunnerFactory() {}

    public static <I, D, O> MRSimpleRunner<I, D, O> prepareRunner(
            Splitter<I> splitter,
            Mapper<D> mapper,
            Reducer<D, O> reducer) {

        return new MRSimpleRunner<>(splitter, mapper, reducer);
    }

    public static <I, D, O> MRSimpleRunner<I, D, O> prepareRunner(
            Splitter<I> splitter,
            Mapper<D> mapper) {

        return new MRSimpleRunner<>(splitter, mapper, null);
    }

    public static class MRSimpleRunner<I, D, O> {
        private int splitRatio;

        private I input;
        private Splitter<I> splitter;
        private Mapper<D> mapper;
        private Reducer<D, O> reducer;

        private MRSimpleRunner(Splitter<I> splitter,
                                Mapper<D> mapper,
                                Reducer<D, O> reducer) {

            this.splitter = splitter;
            this.mapper = mapper;
            this.reducer = reducer;
        }

        public MRSimpleRunner<I, D, O> input(I input) {
            this.input = input;
            return this;
        }

        public MRSimpleRunner<I, D, O> splitRatio(int splitRatio) {
            this.splitRatio = splitRatio;
            return this;
        }

        public List<O> runParallel() {
            requireNonNull(reducer);

            return splitter.split(input, splitRatio)
                    .parallelStream()
                    .map(mapper::map)
                    .collect(toList())
                        .parallelStream()
                        .map(reducer::reduce)
                        .collect(toList());
        }

        public CommonReducer<D> mapSequentially() {
            var mapResult = splitter.split(input, splitRatio)
                    .stream()
                    .map(mapper::map)
                    .collect(toList());

            return new CommonReducer<>(mapResult);
        }

        public static class CommonReducer<D> {
            private List<D> mappedData;

            private CommonReducer(List<D> mappedData) {
                this.mappedData = mappedData;
            }

            public <S> S reduceAll(Reducer<List<D>, S> reducer) {
                return reducer.reduce(mappedData);
            }
        }
    }
}
