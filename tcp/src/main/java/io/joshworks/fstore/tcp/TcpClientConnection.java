package io.joshworks.fstore.tcp;

import io.joshworks.fstore.core.io.buffers.StupidPool;
import io.joshworks.fstore.tcp.internal.Message;
import io.joshworks.fstore.tcp.internal.Response;
import io.joshworks.fstore.tcp.internal.ResponseTable;
import io.joshworks.fstore.tcp.internal.RpcEvent;
import org.xnio.StreamConnection;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static java.util.Objects.requireNonNull;

public class TcpClientConnection extends TcpConnection {

    private final AtomicLong reqids = new AtomicLong();
    private final ResponseTable responseTable;

    public TcpClientConnection(StreamConnection connection, StupidPool writePool, ResponseTable responseTable) {
        super(connection, writePool);
        this.responseTable = responseTable;
    }

    public <T, R> Response<R> request(T data) {
        requireNonNull(data, "Entity must be provided");
        long reqId = reqids.getAndIncrement();
        Message message = new Message(reqId, data);
        Response<R> response = responseTable.newRequest(reqId);
        send(message);
        return response;
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

    /**
     * Creates a proxy instance that delegates calls to the remote node
     *
     * @param timeoutMillis request timeout, less than zero for no timeout
     */
    public <T> T createRpcProxy(Class<T> type, int timeoutMillis, boolean invokeVoidAsync) {
        return (T) Proxy.newProxyInstance(type.getClassLoader(),
                new Class[]{type},
                new RpcProxyHandler(timeoutMillis, invokeVoidAsync));
    }

    private class RpcProxyHandler implements InvocationHandler {

        private final int timeoutMillis;
        private final boolean invokeVoidAsync;

        private RpcProxyHandler(int timeoutMillis, boolean invokeVoidAsync) {
            this.timeoutMillis = timeoutMillis;
            this.invokeVoidAsync = invokeVoidAsync;
        }

        @Override
        public Object invoke(Object proxy, Method method, Object[] args) {
            String methodName = method.getName();
            if (Void.TYPE.equals(method.getReturnType()) && invokeVoidAsync) {
                invokeAsync(methodName, args);
                return null;
            }
            Response<Object> invocation = TcpClientConnection.this.invoke(methodName, args);
            if (method.getReturnType().isAssignableFrom(Future.class)) {
                return invocation;
            }
            if (timeoutMillis < 0) {
                return invocation.get();
            }
            return invocation.get(timeoutMillis, TimeUnit.MILLISECONDS);
        }
    }

}
