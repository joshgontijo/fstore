package io.joshworks.fstore.client;

import io.joshworks.fstore.core.io.IOUtils;
import io.joshworks.fstore.es.shared.EventMap;
import io.joshworks.fstore.es.shared.JsonEvent;
import io.joshworks.fstore.es.shared.NodeInfo;

import java.util.Iterator;
import java.util.List;

//Round robin
public final class PartitionedStreamIterator implements ClientStreamIterator {

    private final List<NodeInfo> nodes;
    private int nodeIdx;

    private PartitionedStreamIterator(List<NodeInfo> nodes) {
        this.nodes = nodes;
    }

    @Override
    public boolean hasNext() {
        for (int i = 0; i < nodes.size(); nodeIdx++) {
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
    public JsonEvent next() {
        if (!hasNext()) {
            return null;
        }
        return nodes.get(nodeIdx).next();
    }

    @Override
    public void close() {
        for (EventStoreIterator iterator : nodes) {
            IOUtils.closeQuietly(iterator);
        }
    }

    @Override
    public EventMap checkpoint() {
        return nodes.stream()
                .map(EventStoreIterator::checkpoint)
                .reduce(EventMap.empty(), EventMap::merge);
    }
}