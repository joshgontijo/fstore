package io.joshworks.eventry.server.cluster;

import io.joshworks.eventry.log.EventRecord;
import io.joshworks.eventry.server.cluster.message.ClusterEvent;

import java.nio.charset.StandardCharsets;
import java.util.Scanner;
import java.util.UUID;

public class JGroupsTest  {

    private static String nodeId = UUID.randomUUID().toString().substring(0, 8);

    public static void main(String[] args) throws InterruptedException {

        Cluster cluster = new Cluster(null, "test", null);
        cluster.register("PING", cm -> {
            System.out.println("RECEIVED PING: " + cm.message());
            cm.reply(new StringMessage(nodeId, "PONG", "pong message").toEvent());
        });
        cluster.register("PONG", cm -> {
            System.out.println("Received PONG: " + cm.message());
        });

        cluster.join();


        Thread output = new Thread(() -> {
            Scanner scanner = new Scanner(System.in);
            String cmd;
            do {
                System.out.print("Enter event [EVENT_TYPE] [MESSAGE]: ");
                cmd = scanner.nextLine();
                final String[] split = cmd.split("\\s+");
                if(split.length != 2) {
                    System.err.println("Invalid message format");
                    continue;
                }
                cluster.otherNodes().forEach(address -> {
                    System.out.println("SENDING TO ADDRESS: " + address);
                    cluster.sendAsync(address, new StringMessage(nodeId, split[0], split[1]).toEvent());
//                    cluster.test(address, new StringMessage(nodeId, split[0], split[1]).toEvent());
                });

            } while (!"exit".equals(cmd));
        });


        output.start();
        output.join();
    }


    private static class StringMessage extends ClusterEvent {

        private final String eventType;
        private final String data;

        private StringMessage(String nodeId, String eventType, String data) {
            super(nodeId);
            this.eventType = eventType;
            this.data = data;
        }

        public EventRecord toEvent() {
            return new EventRecord("TEST", eventType, 0, 0, data.getBytes(StandardCharsets.UTF_8), new byte[0]);
        }
    }


}
