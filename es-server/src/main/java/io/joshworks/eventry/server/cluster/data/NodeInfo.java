package io.joshworks.eventry.server.cluster.data;

import com.google.gson.Gson;

public class NodeInfo {

    private static final Gson gson = new Gson();

    public final String type;
    public final String id;
    public final String host;
    public final int port;

    public NodeInfo(String id, String type, String host, int port) {
        this.id = id;
        this.type = type;
        this.host = host;
        this.port = port;
    }


    public static NodeInfo fromJson(String json) {
        return gson.fromJson(json, NodeInfo.class);
    }

    public String toJson() {
        return gson.toJson(this);
    }


}
