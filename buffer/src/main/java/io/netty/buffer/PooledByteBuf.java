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

package io.netty.buffer;

import io.netty.util.internal.ObjectPool.Handle;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.FileChannel;
import java.nio.channels.GatheringByteChannel;
import java.nio.channels.ScatteringByteChannel;

abstract class PooledByteBuf<T> extends AbstractReferenceCountedByteBuf {

    // Recycler处理器，用于回收对象
    private final Handle<PooledByteBuf<T>> recyclerHandle;

    protected PoolChunk<T> chunk;// 在 Netty 中，使用 Jemalloc 算法管理内存，而 Chunk 是里面的一种内存块
    protected long handle;// 从 Chunk 对象中分配的内存块所处的位置
    /**
     * 内存空间。具体什么样的数据，通过子类设置泛型。
     * 1) PooledDirectByteBuf 和 PooledUnsafeDirectByteBuf 为 ByteBuffer ；
     * 2) PooledHeapByteBuf 和 PooledUnsafeHeapByteBuf 为 byte[]
     * memory 属性，可以被多个 ByteBuf 使用。每个 ByteBuf 使用范围为 [offset, maxLength)
     */
    protected T memory;
    protected int offset;// 使用 memory 的开始位置
    protected int length;// 目前使用 memory 的长度(大小)
    int maxLength;// 最大使用 memory 的长度(大小)
    PoolThreadCache cache;
    // 临时ByteBuff，通过 #internalNioBuffer() 方法生成
    // 作用：JavaNio相关的读写操作都是ByteBuffer，所以临时生成该对象用于与JavaNio的相关API交互
    ByteBuffer tmpNioBuf;
    private ByteBufAllocator allocator;// ByteBuf 分配器

    @SuppressWarnings("unchecked")
    protected PooledByteBuf(Handle<? extends PooledByteBuf<T>> recyclerHandle, int maxCapacity) {
        super(maxCapacity);
        this.recyclerHandle = (Handle<PooledByteBuf<T>>) recyclerHandle;
    }

    // 一般是基于 pooled 的 PoolChunk 对象，初始化 PooledByteBuf 对象
    void init(PoolChunk<T> chunk, ByteBuffer nioBuffer,
              long handle, int offset, int length, int maxLength, PoolThreadCache cache) {
        init0(chunk, nioBuffer, handle, offset, length, maxLength, cache);
    }

    // 基于 unPoolooled 的 PoolChunk 对象，初始化 PooledByteBuf 对象
    void initUnpooled(PoolChunk<T> chunk, int length) {
        init0(chunk, null, 0, 0, length, length, null);
    }

    private void init0(PoolChunk<T> chunk, ByteBuffer nioBuffer,
                       long handle, int offset, int length, int maxLength, PoolThreadCache cache) {
        assert handle >= 0;
        assert chunk != null;
        assert !PoolChunk.isSubpage(handle) || chunk.arena.size2SizeIdx(maxLength) <= chunk.arena.smallMaxSizeIdx :
                "Allocated small sub-page handle for a buffer size that isn't \"small.\"";

        chunk.incrementPinnedMemory(maxLength);
        this.chunk = chunk;
        memory = chunk.memory;
        tmpNioBuf = nioBuffer;
        allocator = chunk.arena.parent;
        this.cache = cache;
        this.handle = handle;
        this.offset = offset;
        this.length = length;
        this.maxLength = maxLength;
    }

    /**
     * Method must be called before reuse this {@link PooledByteBufAllocator}
     */
    final void reuse(int maxCapacity) {
        maxCapacity(maxCapacity);
        resetRefCnt();// 重置引用计数为0
        setIndex0(0, 0);// 重置读写索引为0
        discardMarks();// 重置读写标记位为0
    }

    /**
     * 当前容量的值为 length 属性
     * maxLength 属性，不是表示最大容量。maxCapacity 属性，才是真正表示最大容量。
     * maxLength 属性有什么用？表示占用 memory 的最大容量( 而不是 PooledByteBuf 对象的最大容量 )。
     * 在写入数据超过 maxLength 容量时，会进行扩容，但是容量的上限，为 maxCapacity 。
     */
    @Override
    public final int capacity() {
        return length;
    }

    @Override
    public int maxFastWritableBytes() {
        return Math.min(maxLength, maxCapacity()) - writerIndex;
    }

    @Override
    public final ByteBuf capacity(int newCapacity) {
        if (newCapacity == length) {
            ensureAccessible();
            return this;
        }
        checkNewCapacity(newCapacity);
        // Chunk内存池化，如果要求的新容量未超过memory的最大容量maxLength就无须扩容
        if (!chunk.unpooled) {
            // If the request capacity does not require reallocation, just update the length of the memory.
            if (newCapacity > length) {
                if (newCapacity <= maxLength) {
                    length = newCapacity;
                    return this;
                }
            } else if (newCapacity > maxLength >>> 1 &&
                    (maxLength > 512 || newCapacity > maxLength - 16)) {
                // here newCapacity < length
                length = newCapacity;
                trimIndicesToCapacity(newCapacity);
                return this;
            }
        }

        // 重新分配新的内存空间，并将数据复制到其中，并释放老的内存空间。
        // Reallocation required.
        chunk.decrementPinnedMemory(maxLength);
        chunk.arena.reallocate(this, newCapacity, true);
        return this;
    }

    @Override
    public final ByteBufAllocator alloc() {
        return allocator;
    }

    @Override
    public final ByteOrder order() {
        return ByteOrder.BIG_ENDIAN;
    }

    @Override
    public final ByteBuf unwrap() {
        return null;
    }

    @Override
    public final ByteBuf retainedDuplicate() {
        return PooledDuplicatedByteBuf.newInstance(this, this, readerIndex(), writerIndex());
    }

    @Override
    public final ByteBuf retainedSlice() {
        final int index = readerIndex();
        return retainedSlice(index, writerIndex() - index);
    }

    @Override
    public final ByteBuf retainedSlice(int index, int length) {
        return PooledSlicedByteBuf.newInstance(this, this, index, length);
    }

    protected final ByteBuffer internalNioBuffer() {
        ByteBuffer tmpNioBuf = this.tmpNioBuf;
        if (tmpNioBuf == null) {
            this.tmpNioBuf = tmpNioBuf = newInternalNioBuffer(memory);
        } else {
            tmpNioBuf.clear();
        }
        return tmpNioBuf;
    }

    protected abstract ByteBuffer newInternalNioBuffer(T memory);

    // 当引用计数为 0 时，调用该方法，进行内存回收
    @Override
    protected final void deallocate() {
        if (handle >= 0) {
            // 重置属性
            final long handle = this.handle;
            this.handle = -1;
            memory = null;
            // 释放内存回 Arena 中
            chunk.decrementPinnedMemory(maxLength);
            chunk.arena.free(chunk, tmpNioBuf, handle, maxLength, cache);
            tmpNioBuf = null;
            chunk = null;
            // 回收对象
            recycle();
        }
    }

    private void recycle() {
        recyclerHandle.recycle(this);
    }

    // 获取指定位置在memory变量中的位置
    protected final int idx(int index) {
        return offset + index;
    }

    final ByteBuffer _internalNioBuffer(int index, int length, boolean duplicate) {
        index = idx(index);
        ByteBuffer buffer = duplicate ? newInternalNioBuffer(memory) : internalNioBuffer();
        buffer.limit(index + length).position(index);
        return buffer;
    }

    ByteBuffer duplicateInternalNioBuffer(int index, int length) {
        checkIndex(index, length);
        return _internalNioBuffer(index, length, true);
    }

    @Override
    public final ByteBuffer internalNioBuffer(int index, int length) {
        checkIndex(index, length);
        return _internalNioBuffer(index, length, false);
    }

    @Override
    public final int nioBufferCount() {
        return 1;
    }

    @Override
    public final ByteBuffer nioBuffer(int index, int length) {
        return duplicateInternalNioBuffer(index, length).slice();
    }

    @Override
    public final ByteBuffer[] nioBuffers(int index, int length) {
        return new ByteBuffer[]{nioBuffer(index, length)};
    }

    @Override
    public final boolean isContiguous() {
        return true;
    }

    @Override
    public final int getBytes(int index, GatheringByteChannel out, int length) throws IOException {
        return out.write(duplicateInternalNioBuffer(index, length));
    }

    @Override
    public final int readBytes(GatheringByteChannel out, int length) throws IOException {
        checkReadableBytes(length);
        int readBytes = out.write(_internalNioBuffer(readerIndex, length, false));
        readerIndex += readBytes;
        return readBytes;
    }

    @Override
    public final int getBytes(int index, FileChannel out, long position, int length) throws IOException {
        return out.write(duplicateInternalNioBuffer(index, length), position);
    }

    @Override
    public final int readBytes(FileChannel out, long position, int length) throws IOException {
        checkReadableBytes(length);
        int readBytes = out.write(_internalNioBuffer(readerIndex, length, false), position);
        readerIndex += readBytes;
        return readBytes;
    }

    @Override
    public final int setBytes(int index, ScatteringByteChannel in, int length) throws IOException {
        try {
            return in.read(internalNioBuffer(index, length));
        } catch (ClosedChannelException ignored) {
            return -1;
        }
    }

    @Override
    public final int setBytes(int index, FileChannel in, long position, int length) throws IOException {
        try {
            return in.read(internalNioBuffer(index, length), position);
        } catch (ClosedChannelException ignored) {
            return -1;
        }
    }
}
