package io.joshworks.ilog;

import org.junit.Test;

import java.nio.ByteBuffer;

import static org.junit.Assert.*;

public class Record2Test {


    @Test
    public void create() {
        ByteBuffer rec = RecordUtils.create(1, "abcde");
        int validate = Record2.validate(rec);
    }
}