package io.joshworks.fstore.tcp.server;

import io.joshworks.fstore.tcp.TcpConnection;
import io.joshworks.fstore.tcp.internal.ErrorMessage;
import io.joshworks.fstore.tcp.internal.RpcEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

public class RpcEventHandler implements ServerEventHandler {

    private static final Logger logger = LoggerFactory.getLogger(RpcEventHandler.class);

    private final ServerEventHandler delegate;
    private final Object target;

    public RpcEventHandler(ServerEventHandler delegate, Object target) {
        this.delegate = delegate;
        this.target = target;
    }

    @Override
    public Object onEvent(TcpConnection connection, Object data) {
        if (data instanceof RpcEvent) {
            RpcEvent rpcEvent = (RpcEvent) data;
            if (target != null) {
                return handle(connection, rpcEvent);
            }
            logger.warn("No RPC handler registered for method invocation: " + rpcEvent.methodName);
            //fall through
        }
        return delegate.onEvent(connection, data);
    }

    //TODO improve invocation, method caching, MethodHandle, etc
    private Object handle(TcpConnection connection, RpcEvent event) {
        try {
            Class<?>[] paramTypes = new Class[event.params.length];
            int i = 0;
            for (Object param : event.params) {
                paramTypes[i++] = param.getClass();
            }
            Method method = target.getClass().getMethod(event.methodName, paramTypes);
            method.setAccessible(true);
            return method.invoke(target, event.params);

        } catch (NoSuchMethodException | IllegalAccessException e) {
            String targetName = target.getClass().getSimpleName() + "#" + event.methodName;
            logger.error("Internal RPC call error, peer address: " + connection.peerAddress() + " method: [" + targetName + "]", e);
            return new ErrorMessage(e.getMessage());

        } catch (InvocationTargetException e) {
            String targetName = target.getClass().getSimpleName() + "#" + event.methodName;
            Throwable cause = e.getCause();
            logger.error("RPC method threw an exception, peer address: " + connection.peerAddress() + " method: [" + targetName + "]", cause);
            return new ErrorMessage(cause.getMessage());
        }
    }
}
