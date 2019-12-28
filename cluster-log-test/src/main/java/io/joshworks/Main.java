package io.joshworks;

import io.joshworks.fstore.core.util.FileUtils;

import java.io.File;
import java.io.IOException;

public class Main {

    public static void main(String[] args) throws IOException {

        System.setProperty("java.net.preferIPv4Stack", "true");
        System.setProperty("jgroups.bind_addr", "127.0.0.1");

        File root = new File("S:\\TEST");
        FileUtils.tryDelete(root);

        ReplicatedLog node1 = new ReplicatedLog(root, "node1");
        ReplicatedLog node2 = new ReplicatedLog(root, "node2");
        ReplicatedLog node3 = new ReplicatedLog(root, "node3");

        node1.append(1);
        node2.append(3);
        node3.append(5);

        System.out.println("N1: " + node1.count());
        System.out.println("N2: " + node2.count());
        System.out.println("N3: " + node3.count());

    }

}
