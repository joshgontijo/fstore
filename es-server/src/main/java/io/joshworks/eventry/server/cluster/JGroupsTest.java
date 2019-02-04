package io.joshworks.eventry.server.cluster;

import io.joshworks.eventry.log.EventRecord;
import io.joshworks.eventry.server.cluster.message.command.ClusterCommand;
import io.joshworks.fstore.core.eventbus.EventBus;
import org.jgroups.Address;
import org.jgroups.Message;
import org.jgroups.blocks.RequestHandler;

import java.nio.charset.StandardCharsets;
import java.util.Scanner;

public class JGroupsTest implements RequestHandler {

    public static void main(String[] args) throws InterruptedException {

        EventBus eventBus = new EventBus();
        Cluster cluster = new Cluster("test", eventBus);
        cluster.join();

        Thread output = new Thread(() -> {
            Scanner scanner = new Scanner(System.in);
            String cmd;
            do {
                cmd = scanner.nextLine();
                cluster.cast(new StringMessage(cmd));

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

    private static class StringMessage extends ClusterCommand {

        private final String cmd;

        private StringMessage(String cmd) {
            this.cmd = cmd;
        }

        @Override
        public EventRecord toEvent(Address address) {
            return new EventRecord("YOLO", "MESSAGE_RECEIVED", 0, 0, cmd.getBytes(StandardCharsets.UTF_8), new byte[0]);
        }
    }


}
