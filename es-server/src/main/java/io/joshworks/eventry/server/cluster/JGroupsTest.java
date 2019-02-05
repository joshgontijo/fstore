package io.joshworks.eventry.server.cluster;

import io.joshworks.eventry.log.EventRecord;
import io.joshworks.eventry.server.ClusterEvents;
import io.joshworks.eventry.server.cluster.data.NodeInfo;
import io.joshworks.eventry.server.cluster.message.ClusterEvent;
import io.joshworks.eventry.server.cluster.message.NodeJoined;
import io.joshworks.eventry.server.cluster.message.NodeLeft;
import org.jgroups.Message;
import org.jgroups.blocks.RequestHandler;

import java.nio.charset.StandardCharsets;
import java.util.Scanner;
import java.util.UUID;

public class JGroupsTest implements RequestHandler, ClusterEvents {

    public static void main(String[] args) throws InterruptedException {

        Cluster cluster = new Cluster("test", "node-123");
        cluster.join(new JGroupsTest());
        String nodeId = UUID.randomUUID().toString().substring(0, 8);

        Thread output = new Thread(() -> {
            Scanner scanner = new Scanner(System.in);
            String cmd;
            do {
                cmd = scanner.nextLine();
                cluster.cast(new StringMessage(nodeId, cmd).toEvent());

            } while (!"exit".equals(cmd));
        });


        output.start();
        output.join();
    }

    @Override
    public Object handle(Message msg) {
        System.out.println("RECEIVED: " + new String(msg.buffer(), StandardCharsets.UTF_8));
        return msg;
    }

    @Override
    public void onNodeJoined(NodeJoined nodeJoined) {

    }

    @Override
    public NodeInfo onNodeInfoRequested() {
        return null;
    }

    @Override
    public void onNodeLeft(NodeLeft nodeLeft) {

    }

    private static class StringMessage extends ClusterEvent {

        private final String cmd;

        private StringMessage(String nodeId, String cmd) {
            super(nodeId);
            this.cmd = cmd;
        }

        public EventRecord toEvent() {
            return new EventRecord("YOLO", "MESSAGE_RECEIVED", 0, 0, cmd.getBytes(StandardCharsets.UTF_8), new byte[0]);
        }
    }


}
