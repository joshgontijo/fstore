package io.joshworks.fstore.lsmtree;

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

        System.out.println("aaaaa");
        Thread.sleep(30000);
        for (long i = 0; i < 10; i++) {
            String val = lsmTree.get(i);
            System.out.println(val);
        }

        lsmTree.close();

    }

}
