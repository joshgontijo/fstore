package io.joshworks.eventry.server;

import io.joshworks.eventry.server.subscription.Subscriptions;
import io.joshworks.fstore.core.util.AppProperties;
import io.joshworks.fstore.core.util.FileUtils;
import io.joshworks.fstore.es.shared.EventMap;
import io.joshworks.fstore.es.shared.EventRecord;
import io.joshworks.fstore.es.shared.NodeInfo;
import io.joshworks.fstore.es.shared.subscription.SubscriptionOptions;

import java.io.File;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import static io.joshworks.snappy.SnappyServer.adminPort;
import static io.joshworks.snappy.SnappyServer.cors;
import static io.joshworks.snappy.SnappyServer.get;
import static io.joshworks.snappy.SnappyServer.group;
import static io.joshworks.snappy.SnappyServer.port;
import static io.joshworks.snappy.SnappyServer.post;
import static io.joshworks.snappy.SnappyServer.sse;
import static io.joshworks.snappy.SnappyServer.start;
import static io.joshworks.snappy.SnappyServer.staticFiles;
import static io.joshworks.snappy.http.Response.ok;
import static io.joshworks.snappy.parser.MediaTypes.produces;

public class Server {

    private static final String JAVASCRIPT_MIME = "application/javascript";

    private static final Map<String, NodeInfo> nodes = new ConcurrentHashMap<>();


    public static void main(String[] args) {

        var properties = AppProperties.create();
        var path = properties.get("store.path").orElse("S:\\es-server");
        int serverPort = properties.getInt("port").orElseThrow(() -> new RuntimeException("Port must be provided"));

        FileUtils.tryDelete(new File(path));

        port(serverPort);
        adminPort(serverPort + 10);

        var store = ClusterStore.connect(new File(path), "es-cluster", serverPort);

        var streams = new StreamEndpoint(store);
        var subscriptions = new Subscriptions(3, 3000, 5);

        staticFiles("/", "static");

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

        group("/from-stream", () -> {

        });

        get("/nodelog", req -> {
            List<EventRecord.JsonView> items = store.nodeLog().iterator().stream().map(EventRecord::asJson).collect(Collectors.toList());
            return ok(items);
        }, produces("json"));

        group("/subscriptions", () -> {
//            put("{id}", req -> {
//                String subscriptionId = req.pathParameter("id");
//                Subscription subscription = req.body().asObject(Subscription.class);
//                EventStoreIterator iterator = store.fromStreams(EventMap.empty(), Set.of(subscription.pattern));
//                subscriptions.create(subscriptionId, iterator);
//                return created();
//            });

            get(req -> ok(subscriptions.info()));

            sse("{subscriptionId}", sse -> {
                var subscriptionId = sse.pathParameter("subscriptionId");
                var patterns = new HashSet<>(sse.queryParameters("stream"));
                int batchSize = sse.queryParameterVal("batchSize").asInt().orElse(20);
                boolean compress = sse.queryParameterVal("compress").asBoolean().orElse(false);
//                boolean wrapEvent = sse.queryParameterVal("wrapEvent").asBoolean().orElse(false);

                var options = new SubscriptionOptions(patterns, batchSize, compress);

                var iterator = store.fromStreams(EventMap.empty(), patterns);
                boolean started = subscriptions.create(subscriptionId, options, iterator, sse);
                if (!started) {
                    sse.close();
                }

                sse.keepAlive(15000);
                sse.onClose(() -> subscriptions.remove(subscriptionId));
            });
        });

        cors();
        start();
    }


}
