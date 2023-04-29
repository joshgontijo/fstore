package io.joshworks.fstore.es.shared.routing;

import io.joshworks.fstore.core.hash.Hash;
import io.joshworks.fstore.core.hash.XXHash;
import io.joshworks.fstore.es.shared.Node;

import java.nio.charset.StandardCharsets;
import java.util.List;

public class HashRouter implements Router {

    private static final Hash hasher = new XXHash();

    public static Node select(List<Node> nodes, String stream) {
        int hash = hasher.hash32(stream.getBytes(StandardCharsets.UTF_8));
        return nodes.get(Math.abs(hash) % nodes.size());
    }

    @Override
    public Node route(List<Node> nodes, String stream) {
        int hash = hasher.hash32(stream.getBytes(StandardCharsets.UTF_8));
        return nodes.get(Math.abs(hash) % nodes.size());
    }

}
