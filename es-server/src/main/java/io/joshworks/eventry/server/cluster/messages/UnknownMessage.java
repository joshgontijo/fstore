package io.joshworks.eventry.server.cluster.messages;

/**
 * Used to issue append to command to the partition owner by another node who received the message from the client
 */
public class UnknownMessage implements ClusterMessage {

    public final int unknownCode;

    public UnknownMessage(int unknownCode) {
        this.unknownCode = unknownCode;
    }

}
