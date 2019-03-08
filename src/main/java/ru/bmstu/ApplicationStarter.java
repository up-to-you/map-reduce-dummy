package ru.bmstu;

import static java.lang.String.format;
import static java.lang.System.err;
import static java.lang.System.lineSeparator;
import static java.time.LocalDateTime.now;
import static java.time.temporal.ChronoUnit.SECONDS;
import static ru.bmstu.FileGenerator.generate;

public class ApplicationStarter {

    private static final int DEF_FILE_SIZE = 32_000;
    private static final int DEF_LINE_LENGTH = 150;


    public static void main(String... args) {
        ensureArgs(args);

        var command = args[0];
        var commandArg = args[1];

        switch (command) {
            case "generate": generate(commandArg, DEF_FILE_SIZE, DEF_LINE_LENGTH);
                break;
            case "count": countWords(commandArg, args[2]);
                break;
            case "sort": sortFile(commandArg);
                break;
        }
    }

    private static void ensureArgs(String... args) {
        if(args.length < 2
                || ! args[0].matches("generate||count||sort")
                || (args[0].equals("count") && args.length < 3)) {

            throw new IllegalArgumentException("arguments are missing: [generate [filepath] " +
                    "|| count [filepath] [word] " +
                    "|| sort [filepath]]");
        }
    }

    private static void countWords(String filePath, String targetWord) {
        int sum = MapReduceExecutor.countWordEntries(filePath, targetWord);
        err.println(format("%sMapReduce FOUND %d TOTAL NUMBER OF ENTRIES OF WORD '%s'", lineSeparator().repeat(3), sum, targetWord));
    }

    private static void sortFile(String filePath) {
        var start = now();
        MapReduceExecutor.sortFile(filePath);
        long seconds = start.until(now(), SECONDS);
        err.println(format("Whole process took %d minutes %d seconds", seconds / 60, seconds % 60));
    }
}
