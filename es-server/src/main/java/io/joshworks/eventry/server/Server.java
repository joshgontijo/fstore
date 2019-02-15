package io.joshworks.eventry.server;

import io.joshworks.eventry.server.cluster.ClusterStore;
import io.joshworks.fstore.core.properties.AppProperties;

import java.io.File;
import java.util.UUID;

public class Server {

    private static final String JAVASCRIPT_MIME = "application/javascript";

    public static void main(String[] args) {
        AppProperties properties = AppProperties.create();
        String path = properties.get("store.path").orElse("J:\\event-store\\" + UUID.randomUUID().toString().substring(0, 8));
        ClusterStore store = ClusterStore.connect(new File(path), "test");

    }

}
