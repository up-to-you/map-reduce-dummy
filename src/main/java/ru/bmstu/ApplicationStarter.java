package ru.bmstu;

import static java.util.List.of;

public class ApplicationStarter {

    private static final int DEF_FILE_SIZE = 32_000;
    private static final int DEF_LINE_LENGTH = 150;


    public static void main(String[] args) {
        var argList = of(args);

        if(argList.size() < 2 || ! argList.get(0).matches("generate||count||sort"))
            throw new IllegalArgumentException("arguments are missing: [generate [filepath] || count [word] || sort [filepath]]");

        var command = argList.get(0);
        var commandArg = argList.get(1);

        switch (command) {
            case "generate": FileGenerator.generate(commandArg, DEF_FILE_SIZE, DEF_LINE_LENGTH);
                break;
            case "count":
                break;
            case "sort":
                break;
        }
    }
}
