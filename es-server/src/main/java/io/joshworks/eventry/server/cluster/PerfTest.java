package io.joshworks.eventry.server.cluster;

import org.jgroups.JChannel;
import org.jgroups.Message;
import org.jgroups.ReceiverAdapter;
import org.jgroups.blocks.MessageDispatcher;
import org.jgroups.protocols.TCP_NIO2;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicInteger;

public class PerfTest {

    public static void main(String[] args) throws Exception {

        Server server = new Server();
        server.start();



        try (JChannel channel = new JChannel()) {
            MessageDispatcher dispatcher = new MessageDispatcher(channel, msg -> {
                System.out.println(msg);
                return "AAAA";
            });
            channel.connect("test-cluster");


            long start = System.currentTimeMillis();
            byte[] data = stringOfLength(1024).getBytes(StandardCharsets.UTF_8);
            for (int i = 0; i < 5000000; i++) {
                channel.send(new Message(null, data).setFlag(Message.Flag.RSVP_NB));
//                Object o = dispatcher.sendMessage(server.channel.address(), data, 0, data.length, RequestOptions.SYNC());
                if(i % 100000 == 0) {
                    System.out.println(i);
                }
            }

            System.out.println("TOTAL: " + (System.currentTimeMillis() - start));
        }

        System.out.println(server.counter);

    }

    private static String stringOfLength(int length) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < length; i++)
            sb.append("A");
        return sb.toString();
    }

    private static final class Server {

        public JChannel channel;
        public MessageDispatcher dispatcher;
        public final AtomicInteger counter = new AtomicInteger();

        private void start() throws Exception {

            channel = new JChannel(); // use the default config, udp.xml

//            dispatcher = new MessageDispatcher(channel, msg -> {
////                System.out.println(msg);
//                return "YOLO";
//            });

            channel.connect("test-cluster");



            channel.setReceiver(new ReceiverAdapter() {
                @Override
                public void receive(Message msg) {
                    counter.incrementAndGet();
                }
            });

        }

        private void start2() throws Exception {

            TCP_NIO2 tcp = new TCP_NIO2();
            tcp.start();


            channel = new JChannel(); // use the default config, udp.xml
            channel.connect("test-cluster");
            channel.setReceiver(new ReceiverAdapter() {
                @Override
                public void receive(Message msg) {
                    counter.incrementAndGet();
                }
            });

        }


    }


}
