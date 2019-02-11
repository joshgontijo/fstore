package io.joshworks.eventry.server.cluster.commands;

public interface ClusterMessage {
    byte[] toBytes();

    static boolean isError(int code) {
        return code < 0;
    }

}
