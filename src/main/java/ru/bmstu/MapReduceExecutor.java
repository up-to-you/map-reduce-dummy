package ru.bmstu;

import ru.bmstu.core.MapReduceRunnerFactory;
import ru.bmstu.core.Splitter;

import java.nio.file.Path;

import static java.lang.Runtime.getRuntime;

public class MapReduceExecutor {
    private static final int CPU_NUM = getRuntime().availableProcessors();

    public static long countWordEntries(Path path, String word) {
        MapReduceRunnerFactory.prepareRunner(getFileSplitter(), null, null)
                .input(path)
                .splitRatio(CPU_NUM)
                .run();

        return 0;
    }

    private static Splitter<Path> getFileSplitter() {
        return (path, splitRatio) -> {
            try {

//                iterate(path.toFile().length() / splitRatio, identity())
//                        .limit(splitRatio)
//                        .collect();


//                path.toFile().length() / splitRatio


                return null;
            } catch (RuntimeException e) {
                throw new RuntimeException(e);
            }
        };
    }
}
