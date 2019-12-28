package io.joshworks.fstore.network;

public interface IRpcHandler {

    void doSomething();

    String returnSomething();

    String echo(String param);

    String exception();
}
