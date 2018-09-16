package io.joshworks.eventry.server.cluster;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryonet.Client;
import com.esotericsoftware.kryonet.Connection;
import com.esotericsoftware.kryonet.Listener;
import io.joshworks.eventry.log.EventRecord;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class ClusterClient {

    private final Client client;


    public ClusterClient() {
        this.client = new Client();

        Kryo kryo = client.getKryo();
        kryo.register(EventRecord.class);
        kryo.register(WelcomeMessage.class);
        kryo.register(byte[].class);
    }

    public void discoverNodes() {
        try {

            for (int i = 5000; i < 5005 ; i++) {
                InetAddress inetAddress = client.discoverHost(i, 1000);
                if(inetAddress != null) {

                }
            }



            client.start();
            client.connect(5000, address.getAddress(), address.getPort());

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

}
