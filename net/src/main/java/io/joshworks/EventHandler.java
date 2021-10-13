package io.joshworks;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

import java.util.function.BiConsumer;
import java.util.function.Consumer;

class EventHandler extends SimpleChannelInboundHandler<ByteBuf> {

    private final BiConsumer<ChannelHandlerContext, ByteBuf> onMessage;
    private final BiConsumer<ChannelHandlerContext, Throwable> onError;
    private final Consumer<ChannelHandlerContext> onConnect;
    private final Consumer<ChannelHandlerContext> onDisconnect;

    EventHandler(
            BiConsumer<ChannelHandlerContext, ByteBuf> onMessage,
            BiConsumer<ChannelHandlerContext, Throwable> onError,
            Consumer<ChannelHandlerContext> onConnect,
            Consumer<ChannelHandlerContext> onDisconnect) {

        this.onMessage = onMessage;
        this.onError = onError;
        this.onConnect = onConnect;
        this.onDisconnect = onDisconnect;
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) {
        onConnect.accept(ctx);
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) {
        onDisconnect.accept(ctx);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        onError.accept(ctx, cause);
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, ByteBuf msg) {
        onMessage.accept(ctx, msg);
    }
}
