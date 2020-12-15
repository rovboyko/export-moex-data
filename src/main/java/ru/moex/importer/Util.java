package ru.moex.importer;

public class Util {

    public static void checkArgument(boolean check, String message) {
        if (check) throw new IllegalArgumentException(message);
    }

}
