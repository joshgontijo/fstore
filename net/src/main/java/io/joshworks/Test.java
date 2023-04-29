package io.joshworks;

@SuppressWarnings("unused")
public class Test {

    public static void main(String[] args) throws Exception {

        EventServer server = EventServer.create()
                .idleTimeout(10000)
                .onConnect(ctx -> System.out.println("Connected"))
                .onEvent((ctx, data) -> System.out.println("RECEIVED: " + data))
                .bind(9000);


        EventClient client = EventClient.create()
                .keepAlive(100000)
//                .onConnect(ctx -> ctx.writeAndFlush(new PingMessage()))
                .connect("localhost", 9000);


    }

}
