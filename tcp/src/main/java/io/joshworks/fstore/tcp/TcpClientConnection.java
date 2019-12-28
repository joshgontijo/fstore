package io.joshworks.fstore.tcp;

import io.joshworks.fstore.core.io.buffers.BufferPool;
import io.joshworks.fstore.tcp.internal.Ping;
import io.joshworks.fstore.tcp.internal.Pong;
import io.joshworks.fstore.tcp.internal.Response;
import io.joshworks.fstore.tcp.internal.ResponseTable;
import io.joshworks.fstore.tcp.internal.RpcEvent;
import org.xnio.StreamConnection;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static java.util.Objects.requireNonNull;

public class TcpClientConnection extends TcpConnection {

    private final ResponseTable responseTable;


    public TcpClientConnection(StreamConnection connection, BufferPool writePool, ResponseTable responseTable) {
        super(connection, writePool);
        this.responseTable = responseTable;
    }

    public long ping() {
        Response<Pong> response = request(new Ping());
        return response.get().timestamp;
    }

    public <T, R> Response<R> request(T data) {
        requireNonNull(data, "Entity must be provided");

        try (writePool) {
            ByteBuffer buffer = writePool.allocate();
            Response<R> response = responseTable.newRequest(data, buffer);
            buffer.flip();
            super.write(buffer, false);
            return response;
        }
    }

    /**
     * Expects a return from the server, calling void methods will return null
     */
    public <R> Response<R> invoke(String method, Object... params) {
        RpcEvent event = new RpcEvent(method, params);
        return request(event);
    }

    /**
     * Fire and forget, response from the server is ignored
     */
    public void invokeAsync(String method, Object... params) {
        RpcEvent event = new RpcEvent(method, params);
        send(event);
    }

    public <T> T createRpcProxy(Class<T> type, int timeoutMillis) {
        return (T) Proxy.newProxyInstance(type.getClassLoader(),
                new Class[]{type},
                new RpcProxyHandler(timeoutMillis));
    }

    private class RpcProxyHandler implements InvocationHandler {

        private final int timeoutMillis;

        private RpcProxyHandler(int timeoutMillis) {
            this.timeoutMillis = timeoutMillis;
        }

        @Override
        public Object invoke(Object proxy, Method method, Object[] args) {
            String methodName = method.getName();
            if (Void.TYPE.equals(method.getReturnType())) {
                invokeAsync(methodName, args);
                return null;
            }
            if (Future.class.equals(method.getReturnType())) {
                return TcpClientConnection.this.invoke(methodName, args);
            }
            if (CompletableFuture.class.equals(method.getReturnType())) {
                return TcpClientConnection.this.invoke(methodName, args);
            }
            return TcpClientConnection.this.invoke(methodName, args).get(timeoutMillis, TimeUnit.MILLISECONDS);
        }
    }


}
