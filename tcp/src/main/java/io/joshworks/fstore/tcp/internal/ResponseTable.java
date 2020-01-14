package io.joshworks.fstore.tcp.internal;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class ResponseTable {

    private final Map<Long, Response<?>> table = new ConcurrentHashMap<>();

    //can cause heap to build up if Response#get is not used
    public <T> Response<T> newRequest(long reqId) {
        Response<T> response = new Response<>(reqId, table::remove);
        table.put(reqId, response);
        return response;

    }

    public Response<?> remove(long id) {
        return table.remove(id);
    }

    public void clear() {
        table.clear();
    }
}
