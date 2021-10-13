package io.joshworks;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;

import java.io.Closeable;
import java.nio.ByteBuffer;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import static java.util.Objects.requireNonNull;

public class EventClient implements Closeable {

    private final ChannelFuture channel;
    private final EventLoopGroup workerGroup;

    private EventClient(String host, int port, EventHandler handler) {
        workerGroup = new NioEventLoopGroup();
        channel = connectInternal(host, port, handler, workerGroup);
    }

    public static EventClientBuilder create() {
        return new EventClientBuilder();
    }

    public void send(ByteBuffer data) {
        channel.channel().writeAndFlush(data);
    }

    @Override
    public void close() {
        channel.channel().close();
        workerGroup.shutdownGracefully();
    }

    private static ChannelFuture connectInternal(String host, int port, EventHandler handler, EventLoopGroup workerGroup) {
        try {
            var b = new Bootstrap();
            b.group(workerGroup);
            b.channel(NioSocketChannel.class);
            b.option(ChannelOption.SO_KEEPALIVE, true);
            b.handler(new ChannelInitializer<SocketChannel>() {
                @Override
                public void initChannel(SocketChannel ch) {
                    ch.pipeline().addLast(handler);
                }
            });

            return b.connect(host, port).sync();

        } catch (InterruptedException e) {
            workerGroup.shutdownGracefully();
            Thread.currentThread().interrupt();
            throw new RuntimeException("Failed to connect", e);
        } finally {
            workerGroup.shutdownGracefully();
        }
    }

    public static class EventClientBuilder {
        private BiConsumer<ChannelHandlerContext, ByteBuf> onMessage = (a, b) -> {};
        private BiConsumer<ChannelHandlerContext, Throwable> onError = (a, e) -> e.printStackTrace(System.err);
        private Consumer<ChannelHandlerContext> onConnect = a -> {};
        private Consumer<ChannelHandlerContext> onDisconnect = a -> {};

        private EventClientBuilder() {
        }

        public EventClientBuilder onConnect(Consumer<ChannelHandlerContext> onConnect) {
            this.onConnect = requireNonNull(onConnect);
            return this;
        }

        public EventClientBuilder onEvent(BiConsumer<ChannelHandlerContext, ByteBuf> onMessage) {
            this.onMessage = requireNonNull(onMessage);
            return this;
        }

        public EventClientBuilder onError(BiConsumer<ChannelHandlerContext, Throwable> onError) {
            this.onError = requireNonNull(onError);
            return this;
        }

        public EventClient connect(String host, int port) {
            return new EventClient(
                    host,
                    port,
                    new EventHandler(
                            onMessage,
                            onError,
                            onConnect,
                            onDisconnect
                    )
            );
        }
    }


}
