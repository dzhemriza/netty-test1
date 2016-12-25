package org.netty.experiment.echo.protobuf.server;

import org.netty.experiment.protobuf.echo.stubs.ProtoEchoStubs;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import org.apache.log4j.Logger;

/**
 * Protobuf Echo Server Handler implementation
 */
public class ProtobufEchoServerHandler extends SimpleChannelInboundHandler<ProtoEchoStubs.EchoRequest> {

    private static Logger LOG = Logger.getLogger(ProtobufEchoServerHandler.class);

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, ProtoEchoStubs.EchoRequest msg) throws Exception {
        //LOG.trace("Received echo request: " + msg.getData());
        ProtoEchoStubs.EchoResponse response = ProtoEchoStubs.EchoResponse.newBuilder().setData(msg.getData()).build();
        ctx.writeAndFlush(response);
    }
}
