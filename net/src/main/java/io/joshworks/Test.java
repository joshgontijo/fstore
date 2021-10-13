package io.joshworks;

import io.netty.buffer.ByteBuf;

import java.nio.charset.StandardCharsets;

@SuppressWarnings("unused")
public class Test {

    public static void main(String[] args) {

        EventServer server = EventServer.create()
                .onConnect(ctx -> System.out.println("Connected"))
                .onEvent((ctx, data) -> {
                    System.out.println("RECEIVED");
                    int len = data.readInt();
                    byte[] bytes = new byte[len - 4];
                    data.readBytes(bytes);

                    System.out.println("LEN: " + len + " DATA: " + new String(bytes, StandardCharsets.UTF_8));
                }).bind(9000);


        EventClient client = EventClient.create()
                .onConnect(ctx -> {
                    System.out.println("Connected");

                    byte[] data = "Hello !".getBytes(StandardCharsets.UTF_8);

                    ByteBuf buf = ctx.alloc().buffer(4 + data.length);
                    buf.writeInt(data.length + 4);
                    buf.writeBytes(data);
                    ctx.writeAndFlush(buf);
                    buf.release();


                })
                .connect("localhost", 9000);



    }

}
