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
package io.netty.example.http.helloworld;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.example.util.ServerUtil;
import io.netty.handler.codec.http.HttpServerCodec;
import io.netty.handler.codec.http.HttpServerExpectContinueHandler;
import io.netty.handler.ssl.SslContext;

/**
 * An HTTP server that sends back the content of the received HTTP request
 * in a pretty plaintext form.
 */
public final class HttpHelloWorldServer {

    static final boolean SSL = System.getProperty("ssl") != null;
    static final int PORT = Integer.parseInt(System.getProperty("port", SSL ? "8443" : "8080"));

    public static void main(String[] args) throws Exception {
        // 配置SSL
        final SslContext sslCtx = ServerUtil.buildSslContext();

        // 1.创建两个EventLoopGroup对象
        // boss线程组：用于服务端接受客户端连接，一般1个Acceptor线程就够了
        EventLoopGroup bossGroup = new NioEventLoopGroup(1);
        // worker线程组：用于Channel的IO操作和业务处理，默认线程数=CPU*2
        EventLoopGroup workerGroup = new NioEventLoopGroup();
        try {
            // 2.创建并配置服务端启动器
            ServerBootstrap b = new ServerBootstrap();
            b.option(ChannelOption.SO_BACKLOG, 1024);
            b.group(bossGroup, workerGroup) // 配置主从线程组
                    .channel(NioServerSocketChannel.class) // 配置要使用的Channel类
//                    .handler(new LoggingHandler(LogLevel.INFO))
                    .childHandler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        protected void initChannel(SocketChannel ch) throws Exception {
                            ChannelPipeline p = ch.pipeline();
                            if (sslCtx != null) {
                                p.addLast(sslCtx.newHandler(ch.alloc()));
                            }
                            // 3.设置接入服务端的SocketChannel的处理器管道
                            // 向管道内加入HTTP协议的编码/解码器
                            p.addLast(new HttpServerCodec());
                            p.addLast(new HttpServerExpectContinueHandler());
                            p.addLast(new HttpHelloWorldServerHandler());// 自定义handler
                        }
                    });

            // 4.绑定端口，同步等待
            Channel ch = b.bind(PORT).sync().channel();

            System.err.println("Open your web browser and navigate to " +
                    (SSL ? "https" : "http") + "://127.0.0.1:" + PORT + '/');

            // 5.同步等待直至监听服务端关闭
            ch.closeFuture().sync();
        } finally {
            // 6.优雅的关闭两个线程组
            bossGroup.shutdownGracefully();
            workerGroup.shutdownGracefully();
        }
    }
}
