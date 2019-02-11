package io.joshworks.eventry.server.cluster.client;

import java.util.List;

public class ClusterResponse {

    public final List<NodeResponse> responses;

    public ClusterResponse(List<NodeResponse> responses) {
        this.responses = responses;
    }

    public boolean hasError() {
        return responses.stream().anyMatch(NodeResponse::isError);
    }

}
