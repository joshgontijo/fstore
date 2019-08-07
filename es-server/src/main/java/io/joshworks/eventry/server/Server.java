package io.joshworks.eventry.server;

import io.joshworks.eventry.EventStore;
import io.joshworks.eventry.api.IEventStore;
import io.joshworks.eventry.network.Cluster;
import io.joshworks.eventry.network.ClusterNode;
import io.joshworks.eventry.network.MulticastResponse;
import io.joshworks.eventry.network.NodeStatus;
import io.joshworks.fstore.core.util.AppProperties;
import io.joshworks.fstore.core.util.FileUtils;
import io.joshworks.fstore.es.shared.NodeInfo;

import java.io.File;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import static io.joshworks.snappy.SnappyServer.adminPort;
import static io.joshworks.snappy.SnappyServer.cors;
import static io.joshworks.snappy.SnappyServer.get;
import static io.joshworks.snappy.SnappyServer.group;
import static io.joshworks.snappy.SnappyServer.port;
import static io.joshworks.snappy.SnappyServer.post;
import static io.joshworks.snappy.SnappyServer.start;
import static io.joshworks.snappy.http.Response.ok;
import static io.joshworks.snappy.parser.MediaTypes.produces;

public class Server {

    private static final String JAVASCRIPT_MIME = "application/javascript";

    private static final Map<String, NodeInfo> nodes = new ConcurrentHashMap<>();

    public static void main(String[] args) {

        AppProperties properties = AppProperties.create();
        String path = properties.get("store.path").orElse("S:\\es-server");
        int serverPort = properties.getInt("port").orElseThrow(() -> new RuntimeException("Port must be provided"));

        FileUtils.tryDelete(new File(path));

        port(serverPort);
        adminPort(serverPort + 10);


        ClusterStore store = ClusterStore.connect(new File(path), "es-cluster", serverPort);
        StreamEndpoint streams = new StreamEndpoint(store);

        group("/nodes", () -> {
            get(req -> ok(store.nodesInfo()));
        });

        group("/streams", () -> {
            get(streams::allStreams, produces("json"));
            post(streams::create, produces("json"));
            get("/{stream}/metadata", streams::streamMetadata, produces("json"));
            get("{stream}", streams::event, produces("json"));
            post("{stream}", streams::append, produces("json"));
        });

        group("/from-stream/", () -> {

        });

//        group("/subscriptions", () -> {
//            put("{id}", req -> {
//
//            });
//        });

        cors();
        start();
    }


}
