package io.joshworks.fstore.core;

import java.io.File;
import java.lang.ref.PhantomReference;
import java.lang.ref.Reference;
import java.lang.ref.ReferenceQueue;
import java.util.ArrayList;

public class RefQueueTest {

    public static void main(String[] args) {

        var referenceQueue = new ReferenceQueue<User>();
        var references = new ArrayList<LargeObjectFinalizer<User>>();
        var largeObjects = new ArrayList<User>();

        for (int i = 0; i < 10; ++i) {
            File file = new File("" + i);
            User user = new User(i, file);
            largeObjects.add(user);
            references.add(new LargeObjectFinalizer<>(user, file, referenceQueue));
        }

        largeObjects = null;
        System.gc();

        Reference<?> referenceFromQueue;
        for (PhantomReference<User> reference : references) {
            System.out.println(reference.isEnqueued());
        }

        while ((referenceFromQueue = referenceQueue.poll()) != null) {
            ((LargeObjectFinalizer) referenceFromQueue).finalizeResources();
            referenceFromQueue.clear();
        }

    }


    public static class LargeObjectFinalizer<T> extends PhantomReference<T> {

        private File file;

        public LargeObjectFinalizer(T referent, File file, ReferenceQueue<T> q) {
            super(referent, q);
            this.file = file;
        }

        public void finalizeResources() {
            // free resources
            System.out.println("clearing ... " + file);
        }
    }

    private static class User {
        private final int age;
        private final File file;

        public User(int age, File file) {
            this.age = age;
            this.file = file;
        }
    }

}

