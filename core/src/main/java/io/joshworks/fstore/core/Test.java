package io.joshworks.fstore.core;

import java.nio.ByteBuffer;

public class Test {

    public static void main(String[] args) {

        ByteBuffer b1 = ByteBuffer.allocate(12);
        ByteBuffer b2 = ByteBuffer.allocate(12);


        b1.putLong(2).putInt(1001).flip();
        b2.putLong(2).putInt(1000).flip();

        int diff = b1.compareTo(b2);

        System.out.println(diff);
    }

}
