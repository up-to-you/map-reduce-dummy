package ru.bmstu.core;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayDeque;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.Optional.ofNullable;
import static java.util.stream.Collectors.toCollection;
import static java.util.stream.Stream.generate;

public class KWayData implements AutoCloseable {
    @SuppressWarnings("FieldCanBeLocal")
    private int bufferSize;

    private BufferedReader reader;
    private Path dataPath;
    private ArrayDeque<String> dataBuffer;

    private KWayData(Path dataPath) throws IOException {
        this.reader = Files.newBufferedReader(dataPath);
        this.dataPath = dataPath;
    }

    public static KWayData of(Path dataPath) {
        try {
            return new KWayData(dataPath);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public KWayData prepareBuffer(int bufferSize) {
        this.bufferSize = bufferSize;

        this.dataBuffer = generate(this::readExternalLine)
                .limit(this.bufferSize)
                .collect(toCollection(ArrayDeque::new));

        return this;
    }

    public String getEntry() {
        return dataBuffer.getFirst();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        KWayData that = (KWayData) o;
        return dataPath.equals(that.dataPath);
    }

    @Override
    public int hashCode() {
        return Objects.hash(dataPath);
    }

    @Override
    public void close() {
        try {
            reader.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public boolean ensureBuffer() {
        dataBuffer.removeFirst();

        if(dataBuffer.isEmpty()) {
            generate(this::readExternalLine)
                    .takeWhile(Objects::nonNull)
                    .limit(bufferSize)
                    .forEach(dataBuffer::add);
        }

        return dataBuffer.isEmpty();
    }

    public Path getPath() {
        return dataPath;
    }

    private String readExternalLine() {
        try {
            return reader.readLine();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
