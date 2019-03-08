package ru.bmstu;

import ru.bmstu.core.MRWorkersFactory;
import ru.bmstu.core.Mapper;
import ru.bmstu.core.Reducer;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

import static java.lang.Runtime.getRuntime;
import static ru.bmstu.core.MRRunnerFactory.prepareRunner;

@SuppressWarnings("WeakerAccess")
public final class MapReduceExecutor {
    private static final int CPU_NUM = getRuntime().availableProcessors();

    private static MRWorkersFactory workers = MRWorkersFactory.instance();

    public static int countWordEntries(String path, String word) {
        var counts = prepareRunner(workers::splitFile, workers::mapWordsCount, workers.getWordsReducer(word))
                .input(Paths.get(path))
                .splitRatio(CPU_NUM)
                .runParallel();

        return counts.stream()
                .mapToInt(Integer::intValue)
                .sum();
    }

    public static Path sortFile(String path) {
        var target = Paths.get(path);

        return prepareRunner(workers::splitFile, workers::mapAndSortFilePart)
                .input(target)
                .splitRatio(CPU_NUM)
                .mapSequentially()
                .reduceAll(workers::reduceAndSortFile);
    }
}
