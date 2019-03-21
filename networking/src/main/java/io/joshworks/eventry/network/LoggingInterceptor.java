package io.joshworks.eventry.network;

import org.jgroups.Message;

import java.util.function.BiConsumer;

public class LoggingInterceptor implements BiConsumer<Message, ClusterMessage> {

    @Override
    public void accept(Message message, ClusterMessage entity) {
        System.err.println(String.format("<<<<<< %s => %s", message, entity));
    }
}
