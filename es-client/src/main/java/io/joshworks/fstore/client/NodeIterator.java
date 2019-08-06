package io.joshworks.fstore.client;

import io.joshworks.fstore.es.shared.EventMap;
import io.joshworks.fstore.es.shared.JsonEvent;
import io.joshworks.fstore.es.shared.NodeInfo;
import io.joshworks.restclient.http.HttpResponse;
import io.joshworks.restclient.http.Json;
import io.joshworks.restclient.http.RestClient;

import java.util.ArrayDeque;
import java.util.Queue;

public class NodeIterator implements ClientStreamIterator {

    private static final String SUBSCRIPTION_ENDPOINT = "subscriptions";

    private final String subscriptionId;
    private final NodeInfo node;
    private final RestClient client;
    private final int fetchSize;
    private final Queue<JsonEvent> queue = new ArrayDeque<>();

    public NodeIterator(String subscriptionId, NodeInfo node, RestClient client, int fetchSize) {
        this.subscriptionId = subscriptionId;
        this.node = node;
        this.client = client;
        this.fetchSize = fetchSize;
    }

    private EventMap readSubscription() {
        HttpResponse<Json> response = client.get(node.address, SUBSCRIPTION_ENDPOINT, subscriptionId).asJson();
        if (response.getStatus() == 404) {
            return null;
        }
        if (!response.isSuccessful()) {
            throw new RuntimeException("Could not fetch subscription" + response.getStatus() + ": " + response.asString());
        }
        return
    }

    @Override
    public boolean hasNext() {
        return false;
    }

    @Override
    public JsonEvent next() {
        return null;
    }

    @Override
    public EventMap checkpoint() {
        return nodes.stream()
                .map(EventStoreIterator::checkpoint)
                .reduce(EventMap.empty(), EventMap::merge);
    }

    @Override
    public void close() {

    }

}
