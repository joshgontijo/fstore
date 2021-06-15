package io.joshworks.fstore.ie.server;

import io.joshworks.fstore.core.util.FileUtils;
import io.joshworks.fstore.core.util.TestUtils;
import io.joshworks.fstore.core.util.Threads;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicLong;

public class Main {

    public static void main(String[] args) throws InterruptedException {

        File root = TestUtils.testFolder();
        File master = new File(root, "master");
        File replica1Folder = new File(root, "replica1");
        File replica2Folder = new File(root, "replica2");

        FileUtils.createDir(master);
        FileUtils.createDir(replica1Folder);
        FileUtils.createDir(replica2Folder);

        System.out.println(root);

        int rep1Port = 1346;
        int rep2Port = 1347;

        Replica replica1 = new Replica(replica1Folder, rep1Port);
        Replica replica2 = new Replica(replica2Folder, rep2Port);
        Server server = new Server(master, rep1Port, rep2Port);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            server.close();
            replica1.close();
            replica2.close();
//            TestUtils.deleteRecursively(root);
        }));

        AtomicLong last = new AtomicLong();
        new Thread(() -> {
            while (true) {
                long serverSeq = Server.sequence.get();
                long replicaSeq = Replica.sequence.get();
                long lastSeq = last.get();
                System.out.println(serverSeq + " | " + replicaSeq + " | " + (serverSeq - replicaSeq) + " -> " + (serverSeq - lastSeq));
                last.set(serverSeq);
                Threads.sleep(1000);
            }
        }).start();


        for (int i = 0; i < 1000000000; i++) {
            ByteBuffer record = RecordUtils.create(i, "value-" + i);
            server.append(record, ReplicationLevel.ONE);
//            if (i % 10000 == 0) {
//                Threads.sleep(100);
//            }
//            System.out.println("WRITE SUCCESSFUL: " + i);
//            Threads.sleep(2000);
//            if (i % 50000 == 0) {
//                long now = System.currentTimeMillis();
//                System.out.println("WRITTEN: " + i + " IN " + (now - s));
//                s = now;
//            }

        }


        Threads.sleep(2400000);
//        server.awaitTermination();

    }


}
