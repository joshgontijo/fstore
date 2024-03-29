package io.joshworks.fstore.core.util;

import java.nio.charset.StandardCharsets;

public final class StringUtils {


    public static boolean nonBlank(String val) {
        return !isBlank(val);
    }

    public static boolean isBlank(String val) {
        return val == null || val.trim().isEmpty();
    }

    public static String requireNonBlank(String val) {
        return requireNonBlank(val, "Value");
    }

    public static String requireNonBlank(String val, String name) {
        if (isBlank(val)) {
            throw new IllegalArgumentException(name + " must not be null or empty");
        }
        return val;
    }

    public static int utf8Length(String val) {
        return toUtf8Bytes(val).length;
    }

    public static byte[] toUtf8Bytes(String val) {
        if (val == null) {
            return new byte[0];
        }
        return val.getBytes(StandardCharsets.UTF_8);
    }

}
