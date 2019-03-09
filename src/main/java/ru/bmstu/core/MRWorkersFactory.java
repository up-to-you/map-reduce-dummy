package ru.bmstu.core;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Pattern;

import static java.lang.Integer.MAX_VALUE;
import static java.lang.Integer.valueOf;
import static java.lang.String.format;
import static java.lang.System.err;
import static java.lang.System.out;
import static java.lang.Thread.currentThread;
import static java.lang.Thread.sleep;
import static java.nio.file.Files.newOutputStream;
import static java.util.Comparator.comparing;
import static java.util.concurrent.CompletableFuture.allOf;
import static java.util.concurrent.CompletableFuture.runAsync;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Stream.generate;
import static java.util.stream.Stream.iterate;

public class MRWorkersFactory {
    private MRWorkersFactory() {}

    public static MRWorkersFactory instance() {
        return new MRWorkersFactory();
    }

    public List<Path> splitFile(Path path, int splitRatio) {
        long size = path.toFile().length();
        long chunk = size / splitRatio;

        out.println(format("Splitter for file %s started", path));

        List<Splitter.Range> ranges = iterate(0L, aLong -> aLong + chunk)
                .limit(splitRatio)
                .map(leftRange -> Splitter.range(leftRange, leftRange + chunk))
                .peek(out::println)
                .collect(toList());

        ranges.get(ranges.size() - 1)
                .plusRight(size % splitRatio);

        return ranges.stream()
                .map(range -> divideFile(range, path.toFile()))
                .collect(toList());
    }

    public Map<String, Integer> mapWordsCount(Path path) {
        try {
            out.println(format("Mapper %s started", currThreadNum()));

            var rsMap = new HashMap<String, Integer>();
            try(var reader = Files.newBufferedReader(path)) {
                reader.lines()
                        .forEach(line -> rsMap.merge(line, 1, (count, count2) -> count + count2));
            }

            Files.delete(path);
            out.println(format("File %s removed", path));
            out.println(format("Mapper %s finished", currThreadNum()));

            return rsMap;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public Reducer<Map<String, Integer>, Integer> getWordsReducer(String targetWord) {
        return map -> {
            int wordCount = map.get(targetWord);

            err.println(format("Reducer %s found %d entries for word '%s'",
                    currThreadNum(), wordCount, targetWord));

            return wordCount;
        };
    }

    public KWayData mapAndSortFilePart(Path path) {
        try {
            var lines = Files.readAllLines(path);

            var linesArr = lines.toArray(String[]::new);
            Arrays.parallelSort(linesArr);

            try (var writer = new BufferedWriter(new OutputStreamWriter(newOutputStream(path)))) {
                for (var line: linesArr) {
                    writer.write(line);
                    writer.newLine();
                }
            }
            out.println(format("File %s was sorted", path));

            return KWayData.of(path).prepareBuffer(100);

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public Path reduceAndSortFile(List<KWayData> data) {
        var targetPath = data.get(0).getPath().getParent().resolve("sorted_file");

        out.println(format("Started to sort into %s using k-way external sort algorithm", targetPath));

        var linesBuffer = new LinkedBlockingDeque<String>(100);
        var flag = new AtomicBoolean(true);

        var producer = runAsync(() -> {
            while (! data.isEmpty()) {
                data.stream()
                        .min(comparing(KWayData::getEntry))
                        .ifPresent(kWayData -> {
                            try {
                                linesBuffer.putFirst(kWayData.getEntry());
                            } catch (InterruptedException e) {
                                throw new RuntimeException(e);
                            }

                            if (kWayData.ensureBuffer()) {
                                kWayData.close();
                                data.remove(kWayData);
                            }
                        });
            }
            flag.compareAndSet(true, false);
        });

        var consumer = runAsync(() -> {
            try(var sortedWriter = new BufferedWriter(new OutputStreamWriter(newOutputStream(targetPath)))) {
                while (flag.getAcquire()) {
                    var last = linesBuffer.pollLast();
                    if(last != null) {
                        sortedWriter.write(last);
                        sortedWriter.newLine();
                    }
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });

        allOf(producer, consumer).join();

        out.println(format("Sorting into %s completed !", targetPath));

        return targetPath;
    }

    private Path divideFile(Splitter.Range range, File file) {
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

    private Integer currThreadNum() {
        var matcher = Pattern.compile("\\d").matcher(currentThread().getName());
        return matcher.find() ? valueOf(matcher.group()) : 1;
    }
}
