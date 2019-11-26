package io.joshworks.lsm.server;

import io.joshworks.fstore.core.util.AppProperties;
import io.joshworks.fstore.core.util.FileUtils;

import java.io.File;

public class Main {

    public static void main(String[] args) {

        AppProperties properties = AppProperties.create();

        String clusterName = "test-cluster";
        int tcpPort = properties.getInt("clientPort").orElseThrow();
        int replicationPort = properties.getInt("replicationPort").orElseThrow();
        String storePath = properties.get("path").orElseThrow();

        File rootDir = new File(storePath);
        FileUtils.tryDelete(rootDir);

        Server server = Server.join(rootDir, clusterName, tcpPort, replicationPort);



//        Client client = Client.connect(new InetSocketAddress("localhost", tcpPort));

//        long start = System.currentTimeMillis();
//        int interval = 100000;
//        for (int i = 0; i < 10000000; i++) {
//            client.put(String.valueOf(i), new User("Josh", i));
//            if (i % interval == 0) {
//                long now = System.currentTimeMillis();
//                System.out.println("PUT " + interval + " IN " + (now - start) + " -> " + i);
//                start = now;
//            }
//        }
//
//        start = System.currentTimeMillis();
//        for (int i = 0; i < 10000000; i++) {
//            User user = client.get(String.valueOf(i), User.class);
//            if (i % interval == 0) {
//                long now = System.currentTimeMillis();
//                System.out.println("GET " + interval + " IN " + (now - start) + " -> " + i);
//                start = now;
//            }
//        }


    }



}
