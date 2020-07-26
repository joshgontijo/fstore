package io.joshworks.es;

public class VersionMismatch extends RuntimeException {

    public VersionMismatch(int expected, int current) {
        super("Version mismatch, expected " + expected + " current stream version " + current);
    }
}
