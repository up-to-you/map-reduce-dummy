package ru.bmstu;

import ru.bmstu.core.KWayData;
import ru.bmstu.core.Mapper;
import ru.bmstu.core.Reducer;
import ru.bmstu.core.Splitter;
import ru.bmstu.core.Splitter.Range;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.Map.Entry;
import java.util.stream.Collectors;

import static java.lang.Integer.MAX_VALUE;
import static java.lang.Runtime.getRuntime;
import static java.lang.String.format;
import static java.lang.System.*;
import static java.lang.Thread.currentThread;
import static java.nio.file.Files.newOutputStream;
import static java.util.Comparator.comparing;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;
import static java.util.stream.Stream.generate;
import static java.util.stream.Stream.iterate;
import static ru.bmstu.core.MapReduceRunnerFactory.prepareRunner;

@SuppressWarnings("WeakerAccess")
public final class MapReduceExecutor {
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

    public static void main(String[] args) throws IOException {
        Path dir = Paths.get("/home/owner/map-reduce/");

        List<Path> files = getFileSplitter().split(dir.resolve("some_file"), CPU_NUM);

        files.forEach(path -> {
            try {
                var lines = Files.readAllLines(path);

                lines.toArray(String[]::new);

//                Arrays.parallelSort();
                Collections.sort(lines);
                Files.write(path, lines);

                out.println(format("File %s was sorted", path));

            } catch (IOException e) {
                e.printStackTrace();
            }
        });

        List<KWayData> buffer = files.stream()
                .map(MapReduceExecutor::readFirstLines)
                .collect(toList());

        try(var sortedWriter = Files.newBufferedWriter(dir.resolve("sorted_file"))) {
            while (! buffer.isEmpty()) {
                buffer.stream()
                        .min(comparing(KWayData::getBufferedEntry))
                        .ifPresent(kWayData -> {
                            try {
                                sortedWriter.write(kWayData.getBufferedEntry());
                                sortedWriter.newLine();
                            } catch (IOException e) {
                                throw new RuntimeException(e);
                            }

                            if(kWayData.ensureBuffer()) {
                                kWayData.close();
                                buffer.remove(kWayData);
                            }
                        });
            }
        }
    }

    private static KWayData readFirstLines(Path path) {
        return readFirstLines(path, 10);
    }

    private static KWayData readFirstLines(Path path, int linesNum) {
        return KWayData.of(path).prepareBuffer(linesNum);
    }

    private static Splitter<Path> getFileSplitter() {
        return (path, splitRatio) -> {

            long size = path.toFile().length();
            long chunk = size / splitRatio;

            out.println(format("Splitter for file %s started", path));

            List<Range> ranges = iterate(0L, aLong -> aLong + chunk)
                    .limit(splitRatio)
                    .map(leftRange -> Splitter.range(leftRange, leftRange + chunk))
                    .peek(out::println)
                    .collect(toList());

            ranges.get(ranges.size() - 1)
                    .plusRight(size % splitRatio);

            return ranges.stream()
                    .map(range -> divideFile(range, path.toFile()))
                    .collect(toList());
        };
    }

    private static Path divideFile(Range range, File file) {
        int chunksRatio = (int) (range.size() / MAX_VALUE);

        List<byte[]> chunks = generate(() -> MAX_VALUE - 2)
                .limit(chunksRatio)
                .map(byte[]::new)
                .collect(toList());

        chunks.add(new byte[(int) (range.size() % MAX_VALUE) + chunksRatio * 2]);

        var partFile = Paths.get(format("%s/Part_%s", file.getParent(), UUID.randomUUID()));

        try(var raf = new RandomAccessFile(file, "r");
            var bos = new BufferedOutputStream(newOutputStream(partFile), MAX_VALUE / 10)) {

            raf.seek(range.getLeft());

            for(byte[] chunk : chunks) {
                raf.readFully(chunk);
                bos.write(chunk);
            }
            bos.flush();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        out.println(format("Part file %s was created", partFile));

        return partFile;
    }

    private static Mapper<String, Integer> getWordsCountMapper() {
        return (path) -> {
            try {
                var thread = currentThread().getName();
                out.println(format("Mapper %s started", thread));

                var rsMap = new HashMap<String, Integer>();
                try(var reader = Files.newBufferedReader(path)) {
                    reader.lines()
                            .forEach(line -> rsMap.merge(line, 1, (count, count2) -> count + count2));
                }

                Files.delete(path);
                out.println(format("File %s removed", path));
                out.println(format("Mapper %s finished", thread));

                return rsMap;
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        };
    }

    private static Reducer<String, Integer, Integer> getWordsCountReducer(String word) {
        return map -> {
            int wordCount = map.get(word);

            err.println(format("Reducer %s found %d entries for word '%s'",
                    currentThread().getName(), wordCount, word));

            return wordCount;
        };
    }
}
