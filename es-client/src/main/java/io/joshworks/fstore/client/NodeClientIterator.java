package io.joshworks.fstore.client;

import io.joshworks.fstore.core.io.IOUtils;
import io.joshworks.fstore.es.shared.EventMap;
import io.joshworks.fstore.es.shared.EventRecord;

import java.util.List;

public final class NodeClientIterator implements ClientStreamIterator {

    private final List<NodeIterator> nodes;
    private int nodeIdx;

    NodeClientIterator(List<NodeIterator> nodes) {
        this.nodes = nodes;
    }

    @Override
    public boolean hasNext() {
        for (int i = 0; i < nodes.size(); i++, nodeIdx++) {
            if (nodeIdx >= nodes.size()) {
                nodeIdx = 0;
            }
            if (nodes.get(nodeIdx).hasNext()) {
                return true;
            }
        }
        return false;
    }

    @Override
    public EventRecord next() {
        if (!hasNext()) {
            return null;
        }
        return nodes.get(nodeIdx).next();
    }

    @Override
    public void close() {
        for (NodeIterator iterator : nodes) {
            IOUtils.closeQuietly(iterator);
        }
    }

    @Override
    public EventMap checkpoint() {
        return nodes.stream()
                .map(NodeIterator::checkpoint)
                .reduce(EventMap.empty(), EventMap::merge);
    }
}