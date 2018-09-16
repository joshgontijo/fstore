package io.joshworks.eventry.server.cluster;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryonet.Client;
import com.esotericsoftware.kryonet.Connection;
import com.esotericsoftware.kryonet.Listener;
import io.joshworks.eventry.log.EventRecord;

import java.net.InetSocketAddress;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class TcpClient {

    private final InetSocketAddress address;
    private final Client client;

    private BlockingQueue<Object> queue = new LinkedBlockingQueue<>();

    public TcpClient(InetSocketAddress address) {
        this.address = address;
        this.client = new Client();

        Kryo kryo = client.getKryo();
        kryo.register(EventRecord.class);
        kryo.register(WelcomeMessage.class);
        kryo.register(byte[].class);
    }

    public void connect() {
        try {

            client.addListener(new Listener.ThreadedListener(new Listener() {
                public void received(Connection connection, Object object) {
                    System.out.println("Received: " + object);
                    queue.add(object);
                }
            }));


            client.start();
            client.connect(5000, address.getAddress(), address.getPort());

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public Object send(Object data) {
        try {
            client.sendTCP(data);
            return queue.poll(5, TimeUnit.SECONDS);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }
}
