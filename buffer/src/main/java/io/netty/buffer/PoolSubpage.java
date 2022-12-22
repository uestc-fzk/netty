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

import java.util.concurrent.locks.ReentrantLock;

import static io.netty.buffer.PoolChunk.RUN_OFFSET_SHIFT;
import static io.netty.buffer.PoolChunk.SIZE_SHIFT;
import static io.netty.buffer.PoolChunk.IS_USED_SHIFT;
import static io.netty.buffer.PoolChunk.IS_SUBPAGE_SHIFT;
import static io.netty.buffer.SizeClasses.LOG2_QUANTUM;

final class PoolSubpage<T> implements PoolSubpageMetric {

    final PoolChunk<T> chunk;// 所属chunk
    final int elemSize;// 切分的等量小块内存大小，最小16B
    private final int pageShifts;// 默认13
    private final int runOffset;// 此subpage所属run的首页page在Chunk的位移
    private final int runSize;// 所属run的大小

    /**
     * 位图数组
     * 每个bit位记录一个elem的使用情况
     * 因为elemSize最小为16B，即每个bit可能最小表示16B使用情况，则bitmap初始化时如下：
     * bitmap = new long[runSize >>> 6 + LOG2_QUANTUM]; // 即runSize / 1024
     * 即初始化位图数组时按最大长度初始化，然后再以bitmapLength来计算真正使用的位图长度
     * 我觉得有点多余了，完全可以根据具体使用长度初始化位图数组呀
     */
    private final long[] bitmap;
    // 位图数组实际使用长度= maxNumElems/64 + (maxNumElems % 64 == 0 ? 0 : 1)
    private int bitmapLength;
    private int maxNumElems;// 此run划分的等大小内存块数量 = `runSize / elemSize`

    private int nextAvail;// 下个可使用内存块指针
    private int numAvail;// 此subpage剩余的可分配等大小内存块

    PoolSubpage<T> prev;
    PoolSubpage<T> next;

    boolean doNotDestroy;
    private final ReentrantLock lock = new ReentrantLock();

    // TODO: Test if adding padding helps under contention
    //private long pad0, pad1, pad2, pad3, pad4, pad5, pad6, pad7;

    /**
     * Special constructor that creates a linked list head
     */
    PoolSubpage() {
        chunk = null;
        pageShifts = -1;
        runOffset = -1;
        elemSize = -1;
        runSize = -1;
        bitmap = null;
    }

    PoolSubpage(PoolSubpage<T> head, PoolChunk<T> chunk, int pageShifts, int runOffset, int runSize, int elemSize) {
        this.chunk = chunk;
        this.pageShifts = pageShifts;
        this.runOffset = runOffset;
        this.runSize = runSize;
        this.elemSize = elemSize;
        // 1.初始化位图数组
        // 我觉得这里的初始化完全可以在下面计算使用长度时进行，
        // 需要多少初始化多少，没必要以runSize/1024方式计算最大长度
        bitmap = new long[runSize >>> 6 + LOG2_QUANTUM]; // runSize / 64 / QUANTUM(默认4)

        doNotDestroy = true;
        // 2.将此run按照第1次请求的大小均等划分
        if (elemSize != 0) {
            // 计算elem内存块切分数量
            maxNumElems = numAvail = runSize / elemSize;
            nextAvail = 0;
            // 计算位图数组实际使用长度
            bitmapLength = maxNumElems >>> 6;
            if ((maxNumElems & 63) != 0) {
                bitmapLength++;
            }

            for (int i = 0; i < bitmapLength; i++) {
                bitmap[i] = 0;
            }
        }
        // 3.添加到PoolArena是subpage池数组中，这里就是将此subpage插入给定的head节点之后
        addToPool(head);
    }

    /**
     * Returns the bitmap index of the subpage allocation.
     */
    long allocate() {
        if (numAvail == 0 || !doNotDestroy) {
            return -1;
        }
        // 1.找到未使用的elem的索引bitmapIdx
        final int bitmapIdx = getNextAvail();
        if (bitmapIdx < 0) {
            removeFromPool(); // Subpage appear to be in an invalid state. Remove to prevent repeated errors.
            throw new AssertionError("No next available bitmap index found (bitmapIdx = " + bitmapIdx + "), " +
                    "even though there are supposed to be (numAvail = " + numAvail + ") " +
                    "out of (maxNumElems = " + maxNumElems + ") available indexes.");
        }
        // 2.标记位图中该elem索引为1，即表示已使用
        int q = bitmapIdx >>> 6;
        int r = bitmapIdx & 63;
        assert (bitmap[q] >>> r & 1) == 0;
        bitmap[q] |= 1L << r;
        // 3.如果此subpage分配完了就从subPage池子中移除
        if (--numAvail == 0) {
            removeFromPool();
        }
        // 4.返回此subpage中分配的elem内存块的句柄handle
        return toHandle(bitmapIdx);
    }

    /**
     * @return {@code true} if this subpage is in use.
     * {@code false} if this subpage is not used by its chunk and thus it's OK to be released.
     */
    boolean free(PoolSubpage<T> head, int bitmapIdx) {
        if (elemSize == 0) {
            return true;
        }
        // 1.标记位图中该bitmapIdx索引处为0，表示该elem未使用
        int q = bitmapIdx >>> 6;
        int r = bitmapIdx & 63;
        assert (bitmap[q] >>> r & 1) != 0;
        bitmap[q] ^= 1L << r;

        setNextAvail(bitmapIdx);

        // 2.可使用elem数量numAvail+1
        if (numAvail++ == 0) {
            // 如果之前为0，从allocate()方法知道该subpage已经从PoolArena的subpage池子移除，
            // 此时释放了一个elem，再将其放回池子，供后续内存申请分配
            addToPool(head);
            /* When maxNumElems == 1, the maximum numAvail is also 1.
             * Each of these PoolSubpages will go in here when they do free operation.
             * If they return true directly from here, then the rest of the code will be unreachable
             * and they will not actually be recycled. So return true only on maxNumElems > 1. */
            if (maxNumElems > 1) {
                return true;
            }
        }

        // 3.如果subpage中有elem正在使用则返回true，表示不释放run
        if (numAvail != maxNumElems) {
            return true;
        } else {
            // 4.此时subpage中所有elem都未使用
            // 4.1如果此subpage是该内存规格在PoolArena的subpage池子中唯一的subpage则返回true，表示不释放run
            if (prev == next) {
                return true;
            }

            // 4.2若有其它相同内存规格的subpage从arena的subpage池子移除，并返回false，释放此subpage的run
            doNotDestroy = false;
            removeFromPool();
            return false;
        }
    }

    private void addToPool(PoolSubpage<T> head) {
        assert prev == null && next == null;
        prev = head;
        next = head.next;
        next.prev = this;
        head.next = this;
    }

    private void removeFromPool() {
        assert prev != null && next != null;
        prev.next = next;
        next.prev = prev;
        next = null;
        prev = null;
    }

    private void setNextAvail(int bitmapIdx) {
        nextAvail = bitmapIdx;
    }

    private int getNextAvail() {
        int nextAvail = this.nextAvail;
        if (nextAvail >= 0) {
            this.nextAvail = -1;
            return nextAvail;
        }
        return findNextAvail();
    }
    // 从位图中找到未使用的elem索引
    private int findNextAvail() {
        final long[] bitmap = this.bitmap;
        final int bitmapLength = this.bitmapLength;
        for (int i = 0; i < bitmapLength; i++) {
            long bits = bitmap[i];
            if (~bits != 0) {
                return findNextAvail0(i, bits);
            }
        }
        return -1;
    }

    // 找到未使用的elem索引
    private int findNextAvail0(int i, long bits) {
        final int maxNumElems = this.maxNumElems;
        final int baseVal = i << 6;

        for (int j = 0; j < 64; j++) {
            if ((bits & 1) == 0) {
                int val = baseVal | j;
                if (val < maxNumElems) {
                    return val;
                } else {
                    break;
                }
            }
            bits >>>= 1;
        }
        return -1;
    }

    private long toHandle(int bitmapIdx) {
        int pages = runSize >> pageShifts;
        return (long) runOffset << RUN_OFFSET_SHIFT
                | (long) pages << SIZE_SHIFT
                | 1L << IS_USED_SHIFT
                | 1L << IS_SUBPAGE_SHIFT
                | bitmapIdx;
    }

    @Override
    public String toString() {
        final boolean doNotDestroy;
        final int maxNumElems;
        final int numAvail;
        final int elemSize;
        if (chunk == null) {
            // This is the head so there is no need to synchronize at all as these never change.
            doNotDestroy = true;
            maxNumElems = 0;
            numAvail = 0;
            elemSize = -1;
        } else {
            chunk.arena.lock();
            try {
                if (!this.doNotDestroy) {
                    doNotDestroy = false;
                    // Not used for creating the String.
                    maxNumElems = numAvail = elemSize = -1;
                } else {
                    doNotDestroy = true;
                    maxNumElems = this.maxNumElems;
                    numAvail = this.numAvail;
                    elemSize = this.elemSize;
                }
            } finally {
                chunk.arena.unlock();
            }
        }

        if (!doNotDestroy) {
            return "(" + runOffset + ": not in use)";
        }

        return "(" + runOffset + ": " + (maxNumElems - numAvail) + '/' + maxNumElems +
                ", offset: " + runOffset + ", length: " + runSize + ", elemSize: " + elemSize + ')';
    }

    @Override
    public int maxNumElements() {
        if (chunk == null) {
            // It's the head.
            return 0;
        }
        chunk.arena.lock();
        try {
            return maxNumElems;
        } finally {
            chunk.arena.unlock();
        }
    }

    @Override
    public int numAvailable() {
        if (chunk == null) {
            // It's the head.
            return 0;
        }

        chunk.arena.lock();
        try {
            return numAvail;
        } finally {
            chunk.arena.unlock();
        }
    }

    @Override
    public int elementSize() {
        if (chunk == null) {
            // It's the head.
            return -1;
        }

        chunk.arena.lock();
        try {
            return elemSize;
        } finally {
            chunk.arena.unlock();
        }
    }

    @Override
    public int pageSize() {
        return 1 << pageShifts;
    }

    void destroy() {
        if (chunk != null) {
            chunk.destroy();
        }
    }

    void lock() {
        lock.lock();
    }

    void unlock() {
        lock.unlock();
    }
}
