package io.joshworks.fstore.cluster.rpc;

import org.jgroups.Address;

import java.lang.reflect.Method;
import java.util.function.BiFunction;

public class RpcReceiver {

    public static BiFunction<Address, Object, Object> create(final Object target) {
        return (addr, message) -> {
            RpcMessage msg = (RpcMessage) message;
            try {
                Class<?>[] paramTypes = new Class[msg.params.length];
                int i = 0;
                for (Object param : msg.params) {
                    paramTypes[i++] = param.getClass();
                }
                Method method = target.getClass().getDeclaredMethod(msg.methodName, paramTypes);
                return method.invoke(target, msg.params);
            } catch (Exception e) {
                throw new RuntimeException("Error invoking " + target.getClass().getSimpleName() + "#" + msg.methodName, e);
            }
        };
    }

}
