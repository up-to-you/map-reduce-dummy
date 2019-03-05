package ru.bmstu;

import ru.bmstu.core.Mapper;
import ru.bmstu.core.Reducer;
import ru.bmstu.core.Splitter;
import ru.bmstu.core.Splitter.Range;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static java.lang.Runtime.getRuntime;
import static java.lang.String.format;
import static java.lang.System.*;
import static java.lang.Thread.currentThread;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Stream.iterate;
import static ru.bmstu.core.MapReduceRunnerFactory.prepareRunner;

public class MapReduceExecutor {
    private static final int CPU_NUM = getRuntime().availableProcessors();

    public static int countWordEntries(String path, String word) {
        List<Integer> counts = prepareRunner(getFileSplitter(), getWordsCountMapper(), getWordsCountReducer(word))
                .input(Paths.get(path))
                .splitRatio(CPU_NUM)
                .run();

        return counts.stream()
                .mapToInt(Integer::intValue)
                .sum();
    }

    private static Splitter<Path> getFileSplitter() {
        return (path, splitRatio) -> {

            long size = path.toFile().length();
            long chunk = size / splitRatio;

            out.println(format("Split process for file %s started", path));

            List<Range> ranges = iterate(0L, aLong -> aLong + chunk)
                       .limit(splitRatio)
                       .map(leftRange -> Splitter.range(leftRange, leftRange + chunk))
                       .peek(out::println)
                       .collect(toList());

            ranges.get(ranges.size() - 1)
                    .plusRight(size % splitRatio);

            return ranges;
        };
    }

    private static Mapper<Path, String, Integer> getWordsCountMapper() {
        return (path, range) -> {
            try {

                var thread = currentThread().getName();
                out.println(format("Mapper %s process started", thread));

                var rsMap = new HashMap<String, Integer>();

                var raf = new RandomAccessFile(path.toFile(), "r");
                raf.getChannel();

                while (raf.getFilePointer() <= range.getRight()) {
//                    out.println(raf.getFilePointer());
                    rsMap.merge(raf.readLine(), 1, (count, count2) -> count + count2);
//                    err.println(range.getRight());
                }

                out.println(format("Mapper %s process finished", thread));

                return rsMap;
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        };
    }

    private static Reducer<String, Integer, Integer> getWordsCountReducer(String word) {
        return map -> {
            int wordCount = map.get(word);

            out.println(format("Reducer %s find %d entries for word '%s'",
                    currentThread().getName(), wordCount, word));

            return wordCount;
        };
    }

    public static void main(String[] args) throws IOException {
        var raf = new RandomAccessFile(new File("/home/owner/some_file"), "r");
        try {
            Range range = Splitter.range(29360845633L, 29555252152L);

            out.println(29555252152L - 29360845633L);

            int size = (int) (range.getRight() - range.getLeft());
            byte[] bytes = new byte[size];
//        Range{29360845633,33555252152}
//        read(

            raf.seek(range.getLeft());
            raf.readFully(bytes);
//            raf.read(bytes, (int) range.getLeft(), size);

            Files.write(Paths.get("/home/owner/part_1"), bytes);
        } finally {
            raf.close();
        }

    }
}
