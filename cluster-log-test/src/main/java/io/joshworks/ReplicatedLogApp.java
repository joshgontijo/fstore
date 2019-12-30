package io.joshworks;

import io.joshworks.fstore.core.util.FileUtils;
import io.joshworks.fstore.core.util.Threads;
import io.joshworks.fstore.log.Direction;
import io.joshworks.fstore.serializer.Serializers;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class ReplicatedLogApp {

    private static final Set<ReplicatedLog> nodes = new HashSet<>();

    public static void main(String[] args) throws IOException {

        System.setProperty("java.net.preferIPv4Stack", "true");
        System.setProperty("jgroups.bind_addr", "127.0.0.1");

        File root = new File("S:\\TEST");
        FileUtils.tryDelete(root);

        int clusterSize = 5;

        createCluster(root, clusterSize);

        Threads.sleep(5000);


        Executors.newSingleThreadScheduledExecutor()
                .scheduleAtFixedRate(ReplicatedLogApp::printClusterState, 1, 1, TimeUnit.SECONDS);

        for (int i = 0; i < 10000000; i++) {
            append(i);
//            Threads.sleep(30);
//            printClusterState();
        }



//        append(1);
//        append(2);
//        append(5);

//        printData();

    }

    private static void printClusterState() {
        System.out.println("----------- STATE ----------");
        for (ReplicatedLog node : nodes) {
            System.out.println(node);
        }
    }

    private static void createCluster(File root, int clusterSize) {
        for (int i = 0; i < clusterSize; i++) {
            nodes.add(new ReplicatedLog(root, "node_" + i, clusterSize));
        }
    }

    private static void append(int value) {
        for (ReplicatedLog node : nodes) {
            if (node.leader()) {
                node.append(fromInt(value), ReplicationLevel.QUORUM, null);
                return;
            }
        }
        throw new RuntimeException("No leader");
    }

    private static ByteBuffer fromInt(int val) {
        return ByteBuffer.allocate(Integer.BYTES).putInt(val).flip();
    }

    private static void printData() {
        System.out.println();
        System.out.println("----------- DATA ----------");
        for (ReplicatedLog node : nodes) {
            Integer[] values = node.iterator(Direction.FORWARD).stream()
                    .map(r -> r.read(Serializers.INTEGER))
                    .toArray(Integer[]::new);

            System.out.println(node + " -> " + Arrays.toString(values));


        }
    }

}
