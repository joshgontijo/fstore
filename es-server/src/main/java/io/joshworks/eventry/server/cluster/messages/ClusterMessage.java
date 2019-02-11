package io.joshworks.eventry.server.cluster.messages;

import io.joshworks.fstore.core.Serializer;
import io.joshworks.fstore.serializer.VStringSerializer;

public interface ClusterMessage {

    Serializer<String> vStringSerializer = new VStringSerializer();

    byte[] toBytes();

    static boolean isError(int code) {
        return code < 0;
    }

}
