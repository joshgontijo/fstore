package io.joshworks.fstore.tcp;

public interface RpcProxy {

    void doSomething();

    String returnSomething();

    String echo(String param);

    String exception();
}
