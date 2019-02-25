package io.joshworks.fstore.log.segment.header;


import java.nio.charset.StandardCharsets;
import java.util.Arrays;

public interface Header {

    int BYTES = 1024;

    static void validateMagic(String actualMagic, String expectedMagic) {
        byte[] actual = actualMagic.getBytes(StandardCharsets.UTF_8);
        byte[] expected = expectedMagic.getBytes(StandardCharsets.UTF_8);
        if (!Arrays.equals(expected, actual)) {
            throw new InvalidMagic(expectedMagic, actualMagic);
        }
    }

}
