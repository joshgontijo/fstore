package io.joshworks.eventry.server.cluster;

import io.joshworks.eventry.server.cluster.messages.ClusterMessage;
import io.joshworks.fstore.core.Serializer;
import io.joshworks.fstore.serializer.VStringSerializer;

import java.nio.ByteBuffer;
import java.util.Scanner;
import java.util.UUID;

public class JGroupsTest {

    private static String nodeId = UUID.randomUUID().toString().substring(0, 8);

    private static final int MESSAGE_CODE = 0;
    private static final int PING_CODE = 1;
    private static final int PONG_CODE = 2;

    public static void main(String[] args) throws InterruptedException {

        Cluster cluster = new Cluster("test", nodeId);

        cluster.register(MESSAGE_CODE, nm -> {
            StringMessage stringMessage = nm.as(StringMessage::new);
            System.out.println("RECEIVED MESSAGE: " + stringMessage.data);
        });
        cluster.register(PING_CODE, nm -> {
            StringMessage stringMessage = nm.as(StringMessage::new);
            System.out.println("RECEIVED PING: " + stringMessage.data);
            cluster.client().sendAsync(nm.address, new PongMessage());
        });
        cluster.register(PONG_CODE, nm -> {
            StringMessage stringMessage = nm.as(StringMessage::new);
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

        private static final Serializer<String> serializer = new VStringSerializer();
        private final String data;

        private StringMessage(String data) {
            this.data = data;
        }

        public StringMessage(ByteBuffer bb) {
            this.data = serializer.fromBytes(bb);
        }

        @Override
        public byte[] toBytes() {
            var bb = ByteBuffer.allocate(Integer.BYTES + VStringSerializer.sizeOf(data));
            bb.putInt(code());
            serializer.writeTo(data, bb);
            bb.flip();
            return bb.array();
        }

        @Override
        public int code() {
            return MESSAGE_CODE;
        }
    }

    private static class PingMessage extends StringMessage {

        private PingMessage() {
            super("PING");
        }

        @Override
        public int code() {
            return PING_CODE;
        }
    }

    private static class PongMessage extends StringMessage {

        private PongMessage() {
            super("PONG");
        }

        @Override
        public int code() {
            return PONG_CODE;
        }
    }


}
