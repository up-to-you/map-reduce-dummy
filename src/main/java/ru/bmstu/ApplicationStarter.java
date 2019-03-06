package ru.bmstu;

import static java.lang.String.format;
import static java.lang.System.*;

public class ApplicationStarter {

    private static final int DEF_FILE_SIZE = 32_000;
    private static final int DEF_LINE_LENGTH = 150;


    public static void main(String... args) {
        ensureArgs(args);

        var command = args[0];
        var commandArg = args[1];

        switch (command) {
            case "generate": FileGenerator.generate(commandArg, DEF_FILE_SIZE, DEF_LINE_LENGTH);
                break;
            case "count": {
                    String word = args[2];
                    int sum = MapReduceExecutor.countWordEntries(commandArg, word);
                    err.println(format("%sMapReduce FOUND %d TOTAL NUMBER OF ENTRIES OF WORD '%s'", lineSeparator().repeat(3), sum, word));
                }
                break;
            case "sort": {

                }
                break;
        }
    }

    private static void ensureArgs(String... args) {
        if(args.length < 2
                || ! args[0].matches("generate||count||sort")
                || (args[0].equals("count") && args.length < 3))
            throw new IllegalArgumentException("arguments are missing: [generate [filepath] || count [filepath] [word] || sort [filepath]]");
    }
}
