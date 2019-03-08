package ru.bmstu;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Random;
import java.util.stream.Stream;

import static java.lang.ClassLoader.getSystemResource;
import static java.lang.String.format;
import static java.lang.Thread.sleep;
import static java.nio.file.Files.readAllLines;
import static java.util.concurrent.CompletableFuture.runAsync;

public interface FileGenerator {

    /**
     * @param fileSize size in mb
     */
    static void generate(String path, long fileSize, int lineLen) {
        var words = readThesaurus();

        try {
            Path filePath = Paths.get(path);
            File file = filePath.toFile();

            registerFileWatcher(filePath, 2);

            try(var writer = new BufferedWriter(new FileWriter(file), 819_200)) {
                var random = new Random();

                Stream.generate(() -> random.nextInt(words.size()))
                        .parallel()
                        .takeWhile(integer -> isFileFilled(file, fileSize))
                        .map(words::get)
                        .map(word -> createRepeatedLine(word, lineLen))
                        .forEach(line -> {
                            try {
                                writer.write(line);
                            } catch (IOException e) {
                                e.printStackTrace();
                            }
                        });
            }

        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }

    private static void registerFileWatcher(Path filePath, int secTimeout) {
        runAsync(() -> {
            File file = filePath.toFile();
            long millis = secTimeout * 1000;

            while (true) {
                long currentSize = file.length() / 1_048_576;
                Long gigs = currentSize / 1024;
                System.out.println(format("=%s> %d MB", "=".repeat(gigs.intValue()), currentSize));

                try {
                    sleep(millis);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });
    }

    private static String createRepeatedLine(String word, int lineLen) {
        return word.repeat(lineLen / word.length()).concat(System.lineSeparator());
    }

    private static boolean isFileFilled(File file, long targetSize) {
        return (file.length() / 1_048_576) < targetSize;
    }

    private static List<String> readThesaurus() {
        try {
            Path thesaurus = Paths.get(getSystemResource("words_dict").toURI());
            return readAllLines(thesaurus);

        } catch (URISyntaxException | IOException e) {
            throw new RuntimeException(e);
        }
    }
}
