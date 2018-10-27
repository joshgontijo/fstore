package io.joshworks.eventry.stream.disk;

import io.joshworks.eventry.stream.StreamMetadata;
import io.joshworks.eventry.stream.StreamMetadataSerializer;
import io.joshworks.fstore.lsmtree.LsmTree;
import io.joshworks.fstore.serializer.Serializers;

import java.io.File;

//TODO
public class StreamStore {

    private static final String DIR = "streams";
    private final LsmTree<String, StreamMetadata> store;

    public StreamStore(File root) {
        this.store = LsmTree.of(new File(root, DIR), Serializers.VSTRING, new StreamMetadataSerializer(), 50);
    }
}
