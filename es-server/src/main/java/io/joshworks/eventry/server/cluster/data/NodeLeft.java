package io.joshworks.eventry.server.cluster.data;

import com.google.gson.Gson;
import io.joshworks.eventry.server.cluster.Node;

public class NodeLeft extends Node {

    private static final Gson gson = new Gson();

    public NodeLeft(String id, String host, int port) {
        super(id, host, port);
    }

    public static NodeLeft fromJson(String json) {
        return gson.fromJson(json, NodeLeft.class);
    }

    public String toJson() {
        return gson.toJson(this);
    }

}
