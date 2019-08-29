package io.joshworks.fstore.es.shared.routing;

import io.joshworks.fstore.es.shared.Node;

import java.util.List;

public interface Router {

    Node route(List<Node> nodes, String stream);

}
