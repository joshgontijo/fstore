package io.joshworks.eventry.server.cluster.data;

import com.google.gson.Gson;
import io.joshworks.eventry.server.cluster.Node;

public class NodeJoined extends Node {

    private static final Gson gson = new Gson();

    public NodeJoined(String id, String host, int port) {
        super(id, host, port);
    }

    public static NodeJoined fromJson(String json) {
        return gson.fromJson(json, NodeJoined.class);
    }

    public String toJson() {
        return gson.toJson(this);
    }
}
