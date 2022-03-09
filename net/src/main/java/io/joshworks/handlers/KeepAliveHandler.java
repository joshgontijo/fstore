package io.joshworks.handlers;

import io.joshworks.messages.PingMessage;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.timeout.IdleState;
import io.netty.handler.timeout.IdleStateEvent;

public class KeepAliveHandler extends ChannelDuplexHandler {

    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) {
        if (evt instanceof IdleStateEvent) {
            IdleStateEvent e = (IdleStateEvent) evt;
            if (e.state() == IdleState.READER_IDLE) {
//                System.out.println("Closing");
//                ctx.close();
            } else if (e.state() == IdleState.WRITER_IDLE) {
                System.out.println("Sending ping message");
                ctx.writeAndFlush(new PingMessage());
            }
        }
    }
}