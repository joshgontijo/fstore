package io.joshworks.fstore.tcp;

import io.joshworks.fstore.serializer.kryo.KryoSerializer;
import io.joshworks.fstore.tcp.internal.ErrorMessage;
import io.joshworks.fstore.tcp.internal.Message;
import io.joshworks.fstore.tcp.internal.NullMessage;
import io.joshworks.fstore.tcp.internal.Response;
import io.joshworks.fstore.tcp.internal.RpcEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;

public class TypedEventHandler {

    private TypedHandler instance = new TypedHandler();

    private TypedEventHandler() {

    }

    public static TypedEventHandler builder() {
        return new TypedEventHandler();
    }

    public <T> TypedEventHandler on(Class<T> type, BiConsumer<TcpConnection, T> handler) {
        on(type, (tcpConnection, t) -> {
            handler.accept(tcpConnection, t);
            return null;
        });
        return this;
    }

    public <T> TypedEventHandler on(Class<T> type, BiFunction<TcpConnection, T, Object> handler) {
        instance.handlers.put(type, (BiFunction<TcpConnection, Object, Object>) handler);
        return this;
    }

    public TypedEventHandler registerRpc(Object rpcHandler) {
        RpcHandler handler = new RpcHandler(rpcHandler);
        on(RpcEvent.class, handler::onRpc);
        return this;
    }

    public EventHandler build() {
        return instance;
    }

    private static class TypedHandler implements EventHandler {
        private static final Logger logger = LoggerFactory.getLogger(TypedEventHandler.class);
        private final Map<Class<?>, BiFunction<TcpConnection, Object, Object>> handlers = new ConcurrentHashMap<>();
        private final BiFunction<TcpConnection, Object, Object> NO_OP = (conn, msg) -> {
            logger.warn("No handler for event of type {}", msg.getClass().getSimpleName());
            return null;
        };

        private final BiFunction<TcpConnection, Object, Object> NO_RPC = (conn, msg) -> {
            logger.warn("No RPC handler for event of type {}", msg.getClass().getSimpleName());
            return null;
        };

        public static TypedEventHandler rpcHandler(Object rpcHandler) {
            TypedEventHandler handler = new TypedEventHandler();
            handler.registerRpc(rpcHandler);
            return handler;
        }

        @Override
        public void onEvent(TcpConnection connection, Object data) {
            try {
                data = deserialize((ByteBuffer) data);
                if (data instanceof Message) { //request response
                    handleRequestResponseMessage(connection, (Message) data);
                    return;
                }
                Object resp = handlers.getOrDefault(data.getClass(), NO_OP).apply(connection, data);
                if (resp != null) {
                    logger.warn("Sender does not expect response event of type {} but handler returned {}", data.getClass().getSimpleName(), resp.getClass().getSimpleName());
                }
            } catch (Exception e) {
                logger.error("Error handling event " + data.getClass().getSimpleName(), e);
                ErrorMessage error = new ErrorMessage(e.getMessage());
                connection.send(error, false);
            }
        }

        private void handleRequestResponseMessage(TcpConnection connection, Message msg) {
            Response<?> response = connection.responseTable.remove(msg.id);
            if (response != null) {//is a response message complete and return
                response.complete(msg.data);
                return;
            }

            //request message, send a response
            Object resp = handlers.getOrDefault(msg.data.getClass(), NO_OP).apply(connection, msg.data);
            Object res = resp == null ? new NullMessage() : resp;
            connection.send(new Message(msg.id, res), false);
        }

        private Object deserialize(ByteBuffer buffer) {
            try {
                return KryoSerializer.deserialize(buffer);
            } catch (Exception e) {
                throw new RuntimeException("Error while parsing data", e);
            }
        }
    }

    private static class RpcHandler {

        private static final Logger log = LoggerFactory.getLogger(RpcHandler.class);

        private final Object rpcHandler;

        private RpcHandler(Object rpcHandler) {
            this.rpcHandler = rpcHandler;
        }

        //TODO improve invocation, method caching, MethodHandle, etc
        private Object onRpc(TcpConnection conn, RpcEvent event) {
            try {
                Class<?>[] paramTypes = new Class[event.params.length];
                int i = 0;
                for (Object param : event.params) {
                    paramTypes[i++] = param.getClass();
                }
                Method method = rpcHandler.getClass().getMethod(event.methodName, paramTypes);
                method.setAccessible(true);
                return method.invoke(rpcHandler, event.params);

            } catch (NoSuchMethodException | IllegalAccessException e) {
                String targetName = rpcHandler.getClass().getSimpleName() + "#" + event.methodName;
                log.error("Internal RPC call error, peer address: " + conn.peerAddress() + " method: [" + targetName + "]", e);
                return new ErrorMessage(e.getMessage());

            } catch (InvocationTargetException e) {
                String targetName = rpcHandler.getClass().getSimpleName() + "#" + event.methodName;
                Throwable cause = e.getCause();
                log.error("RPC method threw an exception, peer address: " + conn.peerAddress() + " method: [" + targetName + "]", cause);
                return new ErrorMessage(cause.getMessage());
            }
        }

    }

}
