package ru.moex.importer;

import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class Util {

    public static void checkCondition(boolean check, String message) {
        if (check) throw new IllegalArgumentException(message);
    }

    public static <T> T checkNotNull(T element, String message) {
        return Optional.ofNullable(element).orElseThrow(
                () -> new RuntimeException(message)
        );
    }

    @SafeVarargs
    public static <T> T[] checkAllNotNull(String message, T... elements) {
        Stream.of(elements)
            .forEach(el -> checkNotNull(el, message));
        return elements;
    }

    @SafeVarargs
    public static <T> T[] checkAnyNotNull(String message, T... elements) {
        var nonNullList = Stream.of(elements)
                .filter(Objects::nonNull)
                .collect(Collectors.toList());
        if (nonNullList.isEmpty()) {
            throw new RuntimeException(message);
        } else {
            return elements;
        }
    }
}
