package io.joshworks.fstore.network;

public interface RpcProxy {

    void doSomething();

    String returnSomething();

    String echo(String param);

    String exception();
}
