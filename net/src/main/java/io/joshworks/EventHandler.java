package io.joshworks;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

import java.util.function.BiConsumer;
import java.util.function.Consumer;

class EventHandler extends SimpleChannelInboundHandler<Object> {

    private final BiConsumer<ChannelHandlerContext, Object> onMessage;
    private final BiConsumer<ChannelHandlerContext, Throwable> onError;
    private final Consumer<ChannelHandlerContext> onConnect;
    private final Consumer<ChannelHandlerContext> onDisconnect;

    EventHandler(
            BiConsumer<ChannelHandlerContext, Object> onMessage,
            BiConsumer<ChannelHandlerContext, Throwable> onError,
            Consumer<ChannelHandlerContext> onConnect,
            Consumer<ChannelHandlerContext> onDisconnect) {

        this.onMessage = onMessage;
        this.onError = onError;
        this.onConnect = onConnect;
        this.onDisconnect = onDisconnect;
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        super.channelActive(ctx);
        onConnect.accept(ctx);
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception{
        super.channelInactive(ctx);
        onDisconnect.accept(ctx);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception{
        cause.printStackTrace();
        super.exceptionCaught(ctx, cause);
        onError.accept(ctx, cause);
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, Object msg) {
        onMessage.accept(ctx, msg);
    }
}
