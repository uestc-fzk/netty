/*
 * Copyright 2012 The Netty Project
 *
 * The Netty Project licenses this file to you under the Apache License,
 * version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at:
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package io.netty.example.echo;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandler.Sharable;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.util.CharsetUtil;

import java.util.concurrent.TimeUnit;

/**
 * Handler implementation for the echo server.
 */
@Sharable
public class EchoServerHandler extends ChannelInboundHandlerAdapter {

    @Override
    public void channelRead(final ChannelHandlerContext ctx, Object msg) {
        ByteBuf buf = (ByteBuf) msg;
        System.out.printf("服务端收到来自 %s 消息 %s\n", ctx.channel().remoteAddress(), buf.toString(CharsetUtil.UTF_8));
        if ("ping".equals(buf.toString(CharsetUtil.UTF_8))) {
            // 1.自定义普通任务放入Worker线程池
            ctx.channel().eventLoop().execute(new Runnable() {
                @Override
                public void run() {
                    ctx.writeAndFlush(Unpooled.copiedBuffer("pong", CharsetUtil.UTF_8));
                }
            });

            // 2.自定义定时任务放入Worker线程池
            ctx.channel().eventLoop().schedule(new Runnable() {
                @Override
                public void run() {
                    ctx.writeAndFlush(Unpooled.copiedBuffer("pong", CharsetUtil.UTF_8));
                }
            }, 1, TimeUnit.SECONDS);
        }
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
