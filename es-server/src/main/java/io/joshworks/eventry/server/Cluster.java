package io.joshworks.eventry.server;

import io.joshworks.eventry.server.cluster.TcpServer;
import io.joshworks.fstore.core.eventbus.EventBus;

import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;

public class Cluster {

    public static void main(String[] args) throws Exception {

        int port = ThreadLocalRandom.current().nextInt(5000, 6000);
        System.out.println("PORT: " + port);

        EventBus eventBus = new EventBus(Executors.newSingleThreadExecutor());
        eventBus.register(new NodeListener());


        TcpServer server = new TcpServer(port, eventBus);
        server.start();

        Thread.currentThread().join();


    }


}
