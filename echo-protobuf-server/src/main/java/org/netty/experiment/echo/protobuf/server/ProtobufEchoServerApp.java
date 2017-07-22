package org.netty.experiment.echo.protobuf.server;

import org.netty.experiment.protobuf.echo.stubs.ProtoEchoStubs;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.LengthFieldPrepender;
import io.netty.handler.codec.protobuf.ProtobufDecoder;
import io.netty.handler.codec.protobuf.ProtobufEncoder;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.util.SelfSignedCertificate;
import io.netty.util.concurrent.DefaultEventExecutorGroup;
import org.apache.log4j.Logger;

/**
 * The server implementation
 */
public class ProtobufEchoServerApp {

    private static Logger LOG = Logger.getLogger(ProtobufEchoServerApp.class);
    private static final boolean SSL = System.getProperty("ssl") != null;
    private static final int PORT = Integer.parseInt(System.getProperty("port", "8123"));

    private static void run() throws Exception {
        // Configure SSL.
        final SslContext sslCtx;
        if (SSL) {
            SelfSignedCertificate ssc = new SelfSignedCertificate();
            sslCtx = SslContextBuilder.forServer(ssc.certificate(), ssc.privateKey()).build();
        } else {
            sslCtx = null;
        }

        EventLoopGroup bossGroup = new NioEventLoopGroup(1);
        EventLoopGroup workerGroup = new NioEventLoopGroup(6);
        try {
            DefaultEventExecutorGroup g1 = new DefaultEventExecutorGroup(1);
            ServerBootstrap b = new ServerBootstrap();
            //b.group(bossGroup, workerGroup)
            b.group(bossGroup, workerGroup)
                    .channel(NioServerSocketChannel.class)
                    .option(ChannelOption.SO_BACKLOG, 2048)
                    .option(ChannelOption.SO_KEEPALIVE, true)
                    .option(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT)
                    .handler(new LoggingHandler(LogLevel.ERROR))
                    .childHandler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        public void initChannel(SocketChannel ch) throws Exception {
                            ChannelPipeline p = ch.pipeline();
                            if (sslCtx != null) {
                                p.addLast(sslCtx.newHandler(ch.alloc()));
                            }

                            //p.addLast(new ProtobufVarint32FrameDecoder());
                            p.addLast(new LengthFieldBasedFrameDecoder(83886080, 0, 4, 0, 4));
                            p.addLast(new ProtobufDecoder(ProtoEchoStubs.EchoRequest.getDefaultInstance()));

                            //p.addLast(new ProtobufVarint32LengthFieldPrepender());
                            p.addLast(new LengthFieldPrepender(4));
                            p.addLast(new ProtobufEncoder());

                            //p.addLast(g1, new ProtobufEchoServerHandler());
                            p.addLast(new ProtobufEchoServerHandler());
                        }
                    });

            b.bind(PORT).sync().channel().closeFuture().sync();
        } finally {
            bossGroup.shutdownGracefully();
            workerGroup.shutdownGracefully();
        }
    }

    public static void main(String[] args) {
        try {
            run();
        } catch (Exception e) {
            LOG.fatal(e.getMessage(), e);
        }
    }
}
