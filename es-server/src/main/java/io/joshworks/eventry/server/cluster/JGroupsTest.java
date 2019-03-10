package io.joshworks.eventry.server.cluster;

import io.joshworks.eventry.server.cluster.messages.ClusterMessage;
import io.joshworks.fstore.core.Serializer;
import io.joshworks.fstore.serializer.VStringSerializer;
import io.joshworks.fstore.serializer.kryo.KryoStoreSerializer;

import java.nio.ByteBuffer;
import java.util.Scanner;
import java.util.UUID;

public class JGroupsTest {

    private static String nodeId = UUID.randomUUID().toString().substring(0, 8);

    public static void main(String[] args) throws InterruptedException {

        Cluster cluster = new Cluster("test", nodeId);

        cluster.register(StringMessage.class, nm -> {
            StringMessage stringMessage = nm.get();
            System.out.println("RECEIVED MESSAGE: " + stringMessage.data);
        });
        cluster.register(PingMessage.class, nm -> {
            StringMessage stringMessage = nm.get();
            System.out.println("RECEIVED PING: " + stringMessage.data);
            cluster.client().sendAsync(nm.address, new PongMessage());
        });
        cluster.register(PongMessage.class, nm -> {
            StringMessage stringMessage = nm.get();
            System.out.println("RECEIVED PONG: " + stringMessage.data);
        });

        cluster.join();


        Thread output = new Thread(() -> {
            Scanner scanner = new Scanner(System.in);
            String cmd;
            do {
                System.out.print("Enter event [EVENT_TYPE] [MESSAGE]: ");
                cmd = scanner.nextLine();
                final String[] split = cmd.split("\\s+");
                switch (split[0]) {
                    case "MSG":
                        cluster.client().castAsync(new StringMessage(split[1]));
                        break;
                    case "PING":
                        cluster.client().castAsync(new PingMessage());
                        break;
                    default:
                        System.err.println("Not a valid command");
                }

            } while (!"exit".equals(cmd));
        });


        output.start();
        output.join();
    }


    private static class StringMessage implements ClusterMessage {
        private final String data;

        private StringMessage(String data) {
            this.data = data;
        }
    }

    private static class PingMessage extends StringMessage {
        private PingMessage() {
            super("PING");
        }
    }

    private static class PongMessage extends StringMessage {
        private PongMessage() {
            super("PONG");
        }
    }


}
