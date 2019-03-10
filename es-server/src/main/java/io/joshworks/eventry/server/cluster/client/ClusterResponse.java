package io.joshworks.eventry.server.cluster.client;

import io.joshworks.eventry.server.cluster.NodeMessage;

import java.util.List;

public class ClusterResponse {

    public final List<NodeMessage> responses;

    public ClusterResponse(List<NodeMessage> responses) {
        this.responses = responses;
    }

    public boolean hasError() {
        return responses.stream().anyMatch(NodeMessage::isError);
    }

}
