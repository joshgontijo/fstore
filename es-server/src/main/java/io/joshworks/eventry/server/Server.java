package io.joshworks.eventry.server;

import io.joshworks.fstore.core.properties.AppProperties;

import static io.joshworks.snappy.SnappyServer.cors;
import static io.joshworks.snappy.SnappyServer.delete;
import static io.joshworks.snappy.SnappyServer.get;
import static io.joshworks.snappy.SnappyServer.post;
import static io.joshworks.snappy.SnappyServer.put;
import static io.joshworks.snappy.SnappyServer.sse;
import static io.joshworks.snappy.SnappyServer.start;

public class Server {

    private static final String JAVASCRIPT_MIME = "application/javascript";

    public static void main(String[] args) {

        AppProperties properties = AppProperties.create();
//        String path = properties.get("store.path").orElse("J:\\github-store");
        String path = properties.get("store.path").orElse("S:\\event-store");


        cors();
        start();

    }
}
