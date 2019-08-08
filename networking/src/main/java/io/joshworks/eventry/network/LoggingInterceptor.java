package io.joshworks.eventry.network;

import org.jgroups.Message;

import java.util.function.BiConsumer;

public class LoggingInterceptor implements BiConsumer<Message, Object> {

    @Override
    public void accept(Message message, Object entity) {
        System.err.println(String.format("<<<<<< %s => %s", message, entity));
    }
}
