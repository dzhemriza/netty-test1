package org.netty.experiment.echo.protobuf.client;

import org.netty.experiment.protobuf.echo.stubs.ProtoEchoStubs;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import org.apache.log4j.Logger;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Client handler
 */
public class ProtobufEchoClientHandler extends SimpleChannelInboundHandler<ProtoEchoStubs.EchoResponse> {

    private static Logger LOG = Logger.getLogger(ProtobufEchoClientHandler.class);
    private AtomicInteger roundTrip = new AtomicInteger(0);
    private AtomicInteger messagesPerSecond;
    private final int numberOfTimes;
    private final String MSG = "TEST DATA ... DATA TESTTEST DATA ... DATA TESTTEST DATA ... DATA TESTTEST DATA ... DATA TESTTEST DATA ... DATA TESTTEST DATA ... DATA TESTTEST DATA ... DATA TEST";
    private final boolean dynamicSwitchForAllInOneMessagesSend = false;

    public ProtobufEchoClientHandler(AtomicInteger messagesPerSecond, int numberOfTimes) {
        this.messagesPerSecond = messagesPerSecond;
        this.numberOfTimes = numberOfTimes;
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) {
        // When channel is active send the first message
        if (dynamicSwitchForAllInOneMessagesSend) {
            for (int i = 0; i < numberOfTimes; ++i) {
                ProtoEchoStubs.EchoRequest request = ProtoEchoStubs.EchoRequest.newBuilder().setData(MSG).build();
                ctx.writeAndFlush(request);
            }
        } else {
            ProtoEchoStubs.EchoRequest request = ProtoEchoStubs.EchoRequest.newBuilder().setData(MSG).build();
            ctx.writeAndFlush(request);
        }
    }

    @Override
    public void channelRead0(ChannelHandlerContext ctx, ProtoEchoStubs.EchoResponse response) throws Exception {
        if (roundTrip.get() + 1 >= numberOfTimes) {
            // Close the channel as we make all the required requests to the server
            ctx.close();
            return;
        }

        messagesPerSecond.incrementAndGet();
        roundTrip.incrementAndGet();

        if (!dynamicSwitchForAllInOneMessagesSend) {
            ProtoEchoStubs.EchoRequest request = ProtoEchoStubs.EchoRequest.newBuilder().setData(response.getData()).build();
            ctx.writeAndFlush(request);
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        LOG.error("Unexpected error closing the channel: " + cause.getMessage(), cause);
        ctx.close();
    }
}
