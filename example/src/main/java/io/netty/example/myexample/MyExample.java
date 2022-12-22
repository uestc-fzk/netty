package io.netty.example.myexample;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;

/**
 * @author fzk
 * @datetime 2022-10-24 22:21
 */
public class MyExample {
    public static void main(String[] args) {
        // 传入true表示使用直接内存
        PooledByteBufAllocator allocator = new PooledByteBufAllocator(true);

        ByteBuf buf = allocator.buffer(28<<10, 28<<10);
        buf.clear();

        ByteBuf buf2 = allocator.buffer(1 << 12, 1 << 13);
        buf2.clear();

        ByteBuf buf3 = allocator.buffer(1 << 14, 1 << 14);
        buf3.clear();

        ByteBuf buf4 = allocator.buffer(1 << 19, 1 << 19);// Normal
        allocator.heapBuffer(1,1);
        allocator.directBuffer(1,1);
        allocator.directBuffer(1);
        buf4.clear();
    }
}
