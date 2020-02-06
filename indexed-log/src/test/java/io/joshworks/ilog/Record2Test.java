package io.joshworks.ilog;

import org.junit.Test;

import java.nio.ByteBuffer;

public class Record2Test {
    @Test
    public void create() {

        var key = ByteBuffer.allocate(1024).putLong(123).flip();
        var value = ByteBuffer.allocate(4096).putLong(123).putInt(456).flip();
        var dst = ByteBuffer.allocate(4096);

        int size = Record2.create(key, value, dst);
//        Buffers.offsetPosition(dst, size);


        var keyCopy = ByteBuffer.allocate(1024);
        var valCopy = ByteBuffer.allocate(1024);

        int keyLen = Record2.KEY_LEN.get(dst);
        int checksum = Record2.CHECKSUM.get(dst);
        long timestamp = Record2.TIMESTAMP.get(dst);
        int valLen = Record2.VALUE_LEN.get(dst);
        Record2.KEY.copyTo(dst, keyCopy);
        Record2.VALUE.copyTo(dst, valCopy);

        keyCopy.flip();
        valCopy.flip();

        System.out.println();

    }
}