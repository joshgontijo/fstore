package io.joshworks;

import io.joshworks.messages.PingMessage;

@SuppressWarnings("unused")
public class Test {

    public static void main(String[] args) throws Exception {

        EventServer server = EventServer.create()
                .onConnect(ctx -> System.out.println("Connected"))
                .onEvent((ctx, data) -> System.out.println("RECEIVED: " + data))
                .bind(9000);


        EventClient client = EventClient.create()
//                .onConnect(ctx -> ctx.writeAndFlush(new PingMessage()))
                .connect("localhost", 9000);


    }

}
