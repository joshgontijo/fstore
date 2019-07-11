package io.joshworks.fstore.core;

import java.io.IOException;
import java.util.TreeMap;

public class RuntimeIOException extends RuntimeException {

    public RuntimeIOException(String message) {
        super(message);
    }

    public RuntimeIOException(String message, Throwable cause) {
        super(message, cause);
    }

    public static RuntimeException of(IOException ioex) {
        return new RuntimeIOException(ioex.getMessage(), ioex);
    }

    public static RuntimeException of(String message, IOException ioex) {
        return new RuntimeIOException(message, ioex);
    }

    public static void main(String[] args) {
        TreeMap<Long, Integer> maps = new TreeMap<>();
        maps.put(-1L, 1);
        maps.put(0L, 2);
        maps.put(1L, 3);
        maps.put(2L, 4);
        maps.put(5L, 5);
        maps.put(8L, 6);

        System.out.println(maps.higherKey(1L));
        System.out.println(maps.higherKey(0L));
        System.out.println(maps.higherKey(5L));
        System.out.println(maps.higherKey(8L));
    }
}
