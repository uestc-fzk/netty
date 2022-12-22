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

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.Unpooled;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.example.util.ServerUtil;
import io.netty.handler.ssl.SslContext;
import io.netty.util.CharsetUtil;

/**
 * Sends one message when a connection is open and echoes back any received
 * data to the server.  Simply put, the echo client initiates the ping-pong
 * traffic between the echo client and server by sending the first message to
 * the server.
 */
public final class EchoClient {

    static final String HOST = System.getProperty("host", "127.0.0.1");
    static final int PORT = Integer.parseInt(System.getProperty("port", "8007"));
    static final int SIZE = Integer.parseInt(System.getProperty("size", "256"));

    public static void main(String[] args) throws Exception {
        // 配置SSL
        final SslContext sslCtx = ServerUtil.buildSslContext();

        // Configure the client.
        EventLoopGroup group = new NioEventLoopGroup();
        try {
            // 创建Bootstrap客户端启动器
            Bootstrap b = new Bootstrap();
            b.group(group)
                    .channel(NioSocketChannel.class)    // 设置Channel类型
                    .option(ChannelOption.TCP_NODELAY, true)    // 关闭TCP延迟
                    .handler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        public void initChannel(SocketChannel ch) throws Exception {
                            ChannelPipeline p = ch.pipeline();
                            if (sslCtx != null) {
                                p.addLast(sslCtx.newHandler(ch.alloc(), HOST, PORT));
                            }
                            //p.addLast(new LoggingHandler(LogLevel.INFO));
                            p.addLast(new EchoClientHandler());// 添加自定义处理器
                        }
                    });

            // 连接服务器并等待
            ChannelFuture f = b.connect(HOST, PORT).sync();

            // 发心跳消息测试
            Thread.sleep(100);
            f.channel().writeAndFlush(Unpooled.copiedBuffer("ping", CharsetUtil.UTF_8));
            for (int i = 0; i < 10; i++) {
                Thread.sleep(100);
                f.channel().writeAndFlush(Unpooled.copiedBuffer("ping" + i, CharsetUtil.UTF_8));
            }
            // Wait until the connection is closed.
            f.channel().close().sync(); // 手动关闭通道并等待
//            f.channel().closeFuture().sync(); // 等待直至通道关闭
        } finally {
            // Shut down the event loop to terminate all threads.
            group.shutdownGracefully();
        }
    }
}
