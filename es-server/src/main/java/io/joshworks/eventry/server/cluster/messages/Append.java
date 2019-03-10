package io.joshworks.eventry.server.cluster.messages;

import io.joshworks.eventry.network.ClusterMessage;

/**
 * Used to issue append to command to the partition owner by another node who received the message from the client
 */
public class Append implements ClusterMessage {
}
