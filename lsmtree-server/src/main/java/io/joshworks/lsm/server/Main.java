package io.joshworks.lsm.server;

import io.joshworks.fstore.core.util.FileUtils;
import io.joshworks.lsm.server.client.Client;

import java.io.File;
import java.net.InetSocketAddress;

public class Main {

    public static void main(String[] args) {

        String clusterName = "test-cluster";
        int tcpPort = 9999;
        File rootDir = new File("S:\\lsmtree-server-1");

        FileUtils.tryDelete(rootDir);

        Server server = Server.join(rootDir, clusterName, tcpPort);

        Client client = Client.connect(new InetSocketAddress("localhost", tcpPort));

        long start = System.currentTimeMillis();
        int interval = 100000;
        for (int i = 0; i < 10000000; i++) {
            client.put(String.valueOf(i), new User("Josh", i));
            if (i % interval == 0) {
                long now = System.currentTimeMillis();
                System.out.println("PUT " + interval + " IN " + (now - start) + " -> " + i);
                start = now;
            }
        }

        start = System.currentTimeMillis();
        for (int i = 0; i < 10000000; i++) {
            User user = client.get(String.valueOf(i), User.class);
            if (i % interval == 0) {
                long now = System.currentTimeMillis();
                System.out.println("GET " + interval + " IN " + (now - start) + " -> " + i);
                start = now;
            }
        }





    }

    private static class User {
        public String name;
        public int age;

        public User(String name, int age) {
            this.name = name;
            this.age = age;
        }
    }

}
