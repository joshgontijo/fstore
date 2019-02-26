package io.joshworks.fstore.log.segment.header;

public class InvalidMagic extends HeaderException {

    public InvalidMagic(String expected, String actual) {
        super("Invalid magic: Expected: '" + expected + "', actual: '" + actual + "'");
    }
}
