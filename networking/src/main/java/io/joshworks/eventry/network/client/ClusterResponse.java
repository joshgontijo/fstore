package io.joshworks.eventry.network.client;

import io.joshworks.eventry.network.NodeMessage;

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
