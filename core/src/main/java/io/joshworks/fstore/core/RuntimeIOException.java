package io.joshworks.fstore.core;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
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
        List<Integer> list = new ArrayList<>();
        list.add(1);
        list.add(3);
        list.add(6);
        list.add(7);
        list.add(10);

        int val = Collections.binarySearch(list, 0);
        if(val < 0) {
            System.out.println(Math.abs(val) -1);
        }
        System.out.println(val);

    }
}
