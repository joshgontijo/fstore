package io.joshworks.fstore.ie.server;

import io.joshworks.fstore.core.util.FileUtils;
import io.joshworks.fstore.core.util.TestUtils;

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

        Server server = new Server(master, repPort);
        Replica replica1 = new Replica(replicaFolder, repPort);


        long s = System.currentTimeMillis();
        for (int i = 0; i < 1000000000; i++) {
            server.append(RecordUtils.create(i, "value-" + i));
//            System.out.println("WRITE SUCCESSFUL: " + i);
//            Threads.sleep(2000);
            if (i % 50000 == 0) {
                long now = System.currentTimeMillis();
                System.out.println("WRITTEN: " + i + " IN " + (now - s));
                s = now;
            }

        }


        server.awaitTermination();
        replica1.close();

    }


}
