package io.joshworks.nettytest;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandler.Sharable;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

/**
 * Handler implementation for the echo server.
 */
@Sharable
public class EchoServerHandler extends ChannelInboundHandlerAdapter {

    long total = 0;
    long count = 0;

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
//        ctx.write(msg);
        ByteBuf buff = (ByteBuf)msg;
        total +=  buff.readableBytes();
        if(count++ % 50000 == 0) {
            System.out.println("Received " + count);
        }
        buff.release();
    }

    @Override
    public void channelReadComplete(ChannelHandlerContext ctx) {
        ctx.flush();
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        // Close the connection when an exception is raised.
        cause.printStackTrace();
        ctx.close();
    }
}