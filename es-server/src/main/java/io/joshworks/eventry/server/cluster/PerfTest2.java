package io.joshworks.eventry.server.cluster;

import com.esotericsoftware.kryonet.Client;
import com.esotericsoftware.kryonet.Connection;
import com.esotericsoftware.kryonet.Listener;
import com.esotericsoftware.kryonet.Server;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicInteger;

public class PerfTest2 {

    public static void main(String[] args) throws Exception {

        AtomicInteger counter = new AtomicInteger();
        Server server = new Server();
        server.getKryo().register(byte[].class);
        server.getKryo().register(String.class);
        server.start();
        server.bind(54555);
        server.addListener(new Listener() {
            public void received(Connection connection, Object object) {
                counter.incrementAndGet();
            }
        });



        Client client = new Client();
        client.getKryo().register(byte[].class);
        client.getKryo().register(String.class);
        client.start();
        client.connect(5000, "localhost", 54555);


//        Kryo kryo2 = client.getKryo();
//        kryo2.register(byte[].class);

        long start = System.currentTimeMillis();

        byte[] data = stringOfLength(100).getBytes(StandardCharsets.UTF_8);
        for (int i = 0; i < 5000000; i++) {
            client.sendTCP(stringOfLength(100));

            if (i % 100000 == 0) {
                System.out.println(i);
            }
        }

        System.out.println("TOTAL: " + (System.currentTimeMillis() - start));

        System.out.println(counter);

    }

    private static String stringOfLength(int length) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < length; i++)
            sb.append("A");
        return sb.toString();
    }


}
