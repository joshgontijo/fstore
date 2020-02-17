package io.joshworks.fstore.ie.server;

import io.joshworks.fstore.core.util.FileUtils;
import io.joshworks.fstore.core.util.TestUtils;
import io.joshworks.fstore.core.util.Threads;

import java.io.File;

public class Main {

    public static void main(String[] args) throws InterruptedException {

        File root = TestUtils.testFolder();
        File master = new File(root, "master");
        File replicaFolder = new File(root, "replica");

        FileUtils.deleteIfExists(master);
        FileUtils.deleteIfExists(replicaFolder);
        FileUtils.createDir(master);
        FileUtils.createDir(replicaFolder);

        int repPort = 12345;

        Replica replica1 = new Replica(replicaFolder, repPort);
        Server server = new Server(master, repPort);

        new Thread(() -> {
            while (true) {
                long serverSeq = Server.sequence.get();
                long replicaSeq = Replica.sequence.get();
                System.out.println(serverSeq + " | " + replicaSeq + " | " + (serverSeq - replicaSeq));
                Threads.sleep(1000);
            }
        }).start();


        long s = System.currentTimeMillis();
        for (int i = 0; i < 1000000000; i++) {
            server.append(RecordUtils.create(i, "value-" + i));
            if (i % 10000 == 0) {
                Threads.sleep(100);
            }
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
        replica1.close();

    }


}
