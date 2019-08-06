package io.joshworks.fstore.client;

import io.joshworks.fstore.es.shared.JsonEvent;
import io.joshworks.restclient.http.RestClient;

import java.util.function.Consumer;

public class Subscription {

    private final RestClient client;
    private final String stream;
    private final Consumer<JsonEvent> handler;

    public Subscription(RestClient client, String stream, Consumer<JsonEvent> handler) {
        this.client = client;
        this.stream = stream;
        this.handler = handler;
    }


}
