package io.joshworks.fstore.lsm;

import io.joshworks.fstore.serializer.Serializers;

import java.io.File;

public class Main {

    public static void main(String[] args) throws InterruptedException {
        LsmTree<Long, String> lsmTree = LsmTree.of(new File("J:\\lsmtree"), Serializers.LONG, Serializers.STRING, 5);

        for (long i = 0; i < 10; i++) {
            lsmTree.put(i, String.valueOf(i + "-1"));
        }
        for (long i = 0; i < 10; i++) {
            lsmTree.put(i, String.valueOf(i + "-2"));
        }
        for (long i = 0; i < 10; i++) {
            String val = lsmTree.get(i);
            System.out.println(val);
        }

        Thread.sleep(5000);

        lsmTree.close();

    }

}
