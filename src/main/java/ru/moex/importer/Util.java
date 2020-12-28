package ru.moex.importer;

import java.util.Optional;

public class Util {

    public static void checkCondition(boolean check, String message) {
        if (check) throw new IllegalArgumentException(message);
    }

    public static <T> T checkNotNull(T element, String message) {
        return Optional.ofNullable(element).orElseThrow(
                () -> new RuntimeException(message)
        );
    }

    public static String enrichEndpoint(String endpoint, String parameter) {
        return String.format(endpoint, parameter);
    }
}
