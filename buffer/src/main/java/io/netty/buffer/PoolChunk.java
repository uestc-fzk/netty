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

import io.netty.util.internal.LongCounter;
import io.netty.util.internal.PlatformDependent;

import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 从 PoolChunk 分配 PageRun/PoolSubpage 的算法描述
 * > page  - page是可以分配的最小内存块单位，默认8KB
 * > run   - a run 是一系列 page
 * > chunk - a chunk 是一系列 runs
 * > subpage - 大小为16B~28KB，所以有时候Subpage也包含多个Page（这里是jemalloc4为了进一步解决内存碎片化的问题）
 * <p>
 * 首先分配一个大小为chunkSize的字节数组
 * 每当需要创建给定大小的ByteBuf时，我们都会在字节数组中搜索第一个有足够空间的位置，
 * 并返回一个(long)句柄来编码这个offset信息，然后标记这个内存段保留，因此它始终只被一个 ByteBuf 使用
 * <p>
 * For simplicity all sizes are normalized according to {@link PoolArena#size2SizeIdx(int)} method.
 * This ensures that when we request for memory segments of size > pageSize the normalizedCapacity
 * equals the next nearest size in {@link SizeClasses}.
 * <p>
 * Chunk内存按如下布局：
 * <p>
 * /-----------------\
 * | run             |
 * |                 |
 * |                 |
 * |-----------------|
 * | run             |
 * |                 |
 * |-----------------|
 * | unalloctated    |
 * | (freed)         |
 * |                 |
 * |-----------------|
 * | subpage         |
 * |-----------------|
 * | unallocated     |
 * | (freed)         |
 * | ...             |
 * | ...             |
 * | ...             |
 * |                 |
 * |                 |
 * |                 |
 * \-----------------/
 * <p>
 * handle:
 * -------
 * 一个run的句柄各个位布局如下:
 * <p>
 * oooooooo ooooooos ssssssss ssssssue bbbbbbbb bbbbbbbb bbbbbbbb bbbbbbbb
 * o: runOffset (此run的首页在此Chunk的位移，默认0~511), 15bit
 * s: size (此run包含page数量，1~512), 15bit
 * u: isUsed?, 1bit
 * e: isSubpage?, 1bit
 * b: bitmapIdx of subpage, zero if it's not subpage, 32bit
 *
 * <p>
 * Initialization -
 * 最开始的run是整个chunk.
 * The initial run:
 * runOffset = 0
 * size = chunkSize
 * isUsed = no
 * isSubpage = no
 * bitmapIdx = 0
 *
 * <p>
 * Algorithm: [allocateSubpage(size)]
 * ----------
 * 1) find a not full subpage according to size.
 * if it already exists just return, otherwise allocate a new PoolSubpage and call init()
 * note that this subpage object is added to subpagesPool in the PoolArena when we init() it
 * 2) call subpage.allocate()
 * <p>
 * Algorithm: [free(handle, length, nioBuffer)]
 * ----------
 * 1) if it is a subpage, return the slab back into this subpage
 * 2) 如果subpage未使用或是个run，则free它
 * 3) 合并相邻runs
 */
final class PoolChunk<T> implements PoolChunkMetric {
    private static final int SIZE_BIT_LENGTH = 15;
    private static final int INUSED_BIT_LENGTH = 1;
    private static final int SUBPAGE_BIT_LENGTH = 1;
    private static final int BITMAP_IDX_BIT_LENGTH = 32;

    static final int IS_SUBPAGE_SHIFT = BITMAP_IDX_BIT_LENGTH;
    static final int IS_USED_SHIFT = SUBPAGE_BIT_LENGTH + IS_SUBPAGE_SHIFT;
    static final int SIZE_SHIFT = INUSED_BIT_LENGTH + IS_USED_SHIFT;
    static final int RUN_OFFSET_SHIFT = SIZE_BIT_LENGTH + SIZE_SHIFT;

    final PoolArena<T> arena;// 所属Arena
    final Object base;
    // 当前申请的内存块，比如对于堆内存，T就是一个byte数组，对于直接内存，T就是ByteBuffer，
    // 但无论是哪种形式，其内存大小都默认是4MB
    final T memory;
    final boolean unpooled;// 是否池化

    /**
     * a map 管理所有的runs
     * 每个run，第1页page和最后1页page的runOffset作为key，
     * run的句柄handle作为value都保存于runsAvailMap
     */
    private final LongLongHashMap runsAvailMap;

    /**
     * 每个优先级队列管理着相同大小的runs，以runOffset排序，
     * 所以总用持有较小偏移量的首页page的run进行分配
     * 数组长度为68，即16B...4MB之间的68个内存块规格，储存各个内存规格的run
     */
    private final LongPriorityQueue[] runsAvail;

    private final ReentrantLock runsAvailLock;

    // 此Chunk的所有subPage
    private final PoolSubpage<T>[] subpages;

    /**
     * Accounting of pinned memory – memory that is currently in use by ByteBuf instances.
     */
    private final LongCounter pinnedBytes = PlatformDependent.newLongCounter();

    private final int pageSize;// Page大小默认8KB
    /**
     * 从 1 开始左移到 {@link #pageSize} 的位数。默认 13 ，1 << 13 = 8192 。
     * 具体用途，见 {@link #allocateRun(int)} 方法，计算指定容量所在满二叉树的层级。
     */
    private final int pageShifts;
    private final int chunkSize;// Chunk 内存块占用大小。默认为 4M

    // Use as cache for ByteBuffer created from the memory. These are just duplicates and so are only a container
    // around the memory itself. These are often needed for operations within the Pooled*ByteBuf and so
    // may produce extra GC, which can be greatly reduced by caching the duplicates.
    //
    // This may be null if the PoolChunk is unpooled as pooling the ByteBuffer instances does not make any sense here.
    private final Deque<ByteBuffer> cachedNioBuffers;

    int freeBytes;// 此chunk剩余的空间

    PoolChunkList<T> parent;
    PoolChunk<T> prev;
    PoolChunk<T> next;

    // TODO: Test if adding padding helps under contention
    //private long pad0, pad1, pad2, pad3, pad4, pad5, pad6, pad7;

    @SuppressWarnings("unchecked")
    PoolChunk(PoolArena<T> arena, Object base, T memory, int pageSize, int pageShifts, int chunkSize, int maxPageIdx) {
        unpooled = false;
        this.arena = arena;
        this.base = base;
        this.memory = memory;
        this.pageSize = pageSize;
        this.pageShifts = pageShifts;
        this.chunkSize = chunkSize;
        freeBytes = chunkSize;

        runsAvail = newRunsAvailqueueArray(maxPageIdx);
        runsAvailLock = new ReentrantLock();
        runsAvailMap = new LongLongHashMap(-1);
        subpages = new PoolSubpage[chunkSize >> pageShifts];

        // 插入初始Run, offset = 0, pages = chunkSize / pageSize
        int pages = chunkSize >> pageShifts;
        long initHandle = (long) pages << SIZE_SHIFT;
        insertAvailRun(0, pages, initHandle);

        cachedNioBuffers = new ArrayDeque<ByteBuffer>(8);
    }

    /**
     * Creates a special chunk that is not pooled.
     */
    PoolChunk(PoolArena<T> arena, Object base, T memory, int size) {
        unpooled = true;
        this.arena = arena;
        this.base = base;
        this.memory = memory;
        pageSize = 0;
        pageShifts = 0;
        runsAvailMap = null;
        runsAvail = null;
        runsAvailLock = null;
        subpages = null;
        chunkSize = size;
        cachedNioBuffers = null;
    }

    private static LongPriorityQueue[] newRunsAvailqueueArray(int size) {
        LongPriorityQueue[] queueArray = new LongPriorityQueue[size];
        for (int i = 0; i < queueArray.length; i++) {
            queueArray[i] = new LongPriorityQueue();
        }
        return queueArray;
    }

    private void insertAvailRun(int runOffset, int pages, long handle) {
        // 1.将此run包含的页面数量转换为SizeClasses的sizeIdx，找到该内存规格的run队列
        // floor是指如果页面数无法完全找到对应的sizeIdx则向下找，如9个页面72KB就没有，将其向下找到64KB的sizeIdx 43
        int pageIdxFloor = arena.pages2pageIdxFloor(pages);
        LongPriorityQueue queue = runsAvail[pageIdxFloor];
        queue.offer(handle);// 将此run加入该队列中

        // 2.插入每个run的第1页和最后1页到runsAvailMap中
        insertAvailRun0(runOffset, handle);
        if (pages > 1) {
            //insert last page of run
            insertAvailRun0(lastPage(runOffset, pages), handle);
        }
    }

    private void insertAvailRun0(int runOffset, long handle) {
        long pre = runsAvailMap.put(runOffset, handle);
        assert pre == -1;
    }

    private void removeAvailRun(long handle) {
        // 1.将此run包含的页面数量转换为SizeClasses的sizeIdx，找到该内存规格的run队列
        // floor是指如果页面数无法完全找到对应的sizeIdx则向下找，如9个页面72KB就没有，将其向下找到64KB的sizeIdx 43
        int pageIdxFloor = arena.pages2pageIdxFloor(runPages(handle));
        LongPriorityQueue queue = runsAvail[pageIdxFloor];
        // 2.从队列移除该run的句柄handle
        removeAvailRun(queue, handle);
    }

    private void removeAvailRun(LongPriorityQueue queue, long handle) {
        queue.remove(handle);// 队列中移除即可

        int runOffset = runOffset(handle);
        int pages = runPages(handle);
        //remove first page of run
        runsAvailMap.remove(runOffset);
        if (pages > 1) {
            //remove last page of run
            runsAvailMap.remove(lastPage(runOffset, pages));
        }
    }

    private static int lastPage(int runOffset, int pages) {
        return runOffset + pages - 1;
    }

    private long getAvailRunByOffset(int runOffset) {
        return runsAvailMap.get(runOffset);
    }

    @Override
    public int usage() {
        final int freeBytes;
        arena.lock();
        try {
            freeBytes = this.freeBytes;
        } finally {
            arena.unlock();
        }
        return usage(freeBytes);
    }

    private int usage(int freeBytes) {
        if (freeBytes == 0) {
            return 100;
        }

        int freePercentage = (int) (freeBytes * 100L / chunkSize);
        if (freePercentage == 0) {
            return 99;
        }
        return 100 - freePercentage;
    }

    // 从Chunk中分配内存
    boolean allocate(PooledByteBuf<T> buf, int reqCapacity, int sizeIdx, PoolThreadCache cache) {
        final long handle;
        if (sizeIdx <= arena.smallMaxSizeIdx) {
            // 1.small内存块分配，则尝试从某个page分配subpage
            handle = allocateSubpage(sizeIdx);
            if (handle < 0) {
                return false;
            }
            assert isSubpage(handle);
        } else {
            // 2.normal内存块分配
            // runSize必须是pageSize倍数
            int runSize = arena.sizeIdx2size(sizeIdx);
            handle = allocateRun(runSize);
            if (handle < 0) {
                return false;
            }
            assert !isSubpage(handle);
        }

        // 3.将分配到的内存块初始化到buf中
        ByteBuffer nioBuffer = cachedNioBuffers != null ? cachedNioBuffers.pollLast() : null;
        initBuf(buf, nioBuffer, handle, reqCapacity, cache);
        return true;
    }

    /**
     * 1) 找到在runsAvails中第1个满足的run
     * 2) 如果找到的run的pages大于请求的pages，则分裂该run，并保存剩下的run之后使用
     */
    private long allocateRun(int runSize) {
        // 1.计算请求的runSize需要page页数量
        int pages = runSize >> pageShifts;// 右移13位，即除以pageSize 8K
        // 根据需要page数量计算SizeClasses的sizeIdx，作为从runAvail数组寻找的起始偏移量
        int pageIdx = arena.pages2pageIdx(pages);

        runsAvailLock.lock();
        try {
            // 2.从runsAvail中找到第1个满足该请求页面数量大小的run的队列
            int queueIdx = runFirstBestFit(pageIdx);
            if (queueIdx == -1) {
                return -1;
            }

            // 3.获取队列第1个run，即最小runOffset的run
            LongPriorityQueue queue = runsAvail[queueIdx];
            long handle = queue.poll();// 弹出队列第1个run

            assert handle != LongPriorityQueue.NO_VALUE && !isUsed(handle) : "invalid handle: " + handle;
            // 4.从runAvail队列中移除选出的这个run
            removeAvailRun(queue, handle);

            // 5.按照请求的页面数量将此run分裂，返回分裂后请求到的run句柄
            if (handle != -1) {
                handle = splitLargeRun(handle, pages);
            }
            // 剩余空间减少
            int pinnedSize = runSize(pageShifts, handle);
            freeBytes -= pinnedSize;
            return handle;
        } finally {
            runsAvailLock.unlock();
        }
    }

    private int calculateRunSize(int sizeIdx) {
        int maxElements = 1 << pageShifts - SizeClasses.LOG2_QUANTUM;
        int runSize = 0;
        int nElements;

        final int elemSize = arena.sizeIdx2size(sizeIdx);

        //find lowest common multiple of pageSize and elemSize
        do {
            runSize += pageSize;
            nElements = runSize / elemSize;
        } while (nElements < maxElements && runSize != nElements * elemSize);

        while (nElements > maxElements) {
            runSize -= pageSize;
            nElements = runSize / elemSize;
        }

        assert nElements > 0;
        assert runSize <= chunkSize;
        assert runSize >= elemSize;

        return runSize;
    }

    private int runFirstBestFit(int pageIdx) {
        if (freeBytes == chunkSize) {
            return arena.nPSizes - 1;
        }
        for (int i = pageIdx; i < arena.nPSizes; i++) {
            LongPriorityQueue queue = runsAvail[i];
            if (queue != null && !queue.isEmpty()) {
                return i;
            }
        }
        return -1;
    }

    private long splitLargeRun(long handle, int needPages) {
        assert needPages > 0;
        // 此run含有的总的页面数
        int totalPages = runPages(handle);
        assert needPages <= totalPages;
        // 分配请求的页面数后剩余的页面数
        int remPages = totalPages - needPages;
        // 如果Run切分后有剩余页面，则将剩余Run插入runAvails
        if (remPages > 0) {// run分裂，将剩下的run插入availRun和runasAvailMap中
            int runOffset = runOffset(handle);

            // 1.计算剩余run的句柄handle并插入runAvails
            int availOffset = runOffset + needPages;// 剩下的首页面位移
            long availRun = toRunHandle(availOffset, remPages, 0);// 分配后的run的新句柄handle
            insertAvailRun(availOffset, remPages, availRun);// 将分裂后的run插入availRun队列和runsAvailMap中

            // 2.计算并返回划分出来的run的句柄
            return toRunHandle(runOffset, needPages, 1);// 计算分配出去的run的句柄handle
        }

        //mark it as used
        handle |= 1L << IS_USED_SHIFT;
        return handle;
    }

    /**
     * Create / initialize a new PoolSubpage of normCapacity. Any PoolSubpage created / initialized here is added to
     * subpage pool in the PoolArena that owns this PoolChunk
     *
     * @param sizeIdx sizeIdx of normalized size
     * @return index in memoryMap
     */
    private long allocateSubpage(int sizeIdx) {
        // Obtain the head of the PoolSubPage pool that is owned by the PoolArena and synchronize on it.
        // This is need as we may add it back and so alter the linked-list structure.
        // 1.获取 PoolArena持有的 smallSubpagePools 池的sizeIdx队列的头部节点，准备将新分配的subPage插入该队列
        PoolSubpage<T> head = arena.findSubpagePoolHead(sizeIdx);
        head.lock();
        try {
            // 2.分配一个新run
            int runSize = calculateRunSize(sizeIdx);
            //runSize must be multiples of pageSize
            long runHandle = allocateRun(runSize);
            if (runHandle < 0) {
                return -1;
            }

            int runOffset = runOffset(runHandle);
            assert subpages[runOffset] == null;
            int elemSize = arena.sizeIdx2size(sizeIdx);
            // 3.将run新建SubPage，并将其添加到该head节点后面，即放入PoolArena的subpagesPool池中
            PoolSubpage<T> subpage = new PoolSubpage<T>(head, this, pageShifts, runOffset,
                    runSize(pageShifts, runHandle), elemSize);

            subpages[runOffset] = subpage;
            return subpage.allocate();// 这里不用传入大小，是因为该subpage已经根据第1次请求的大小划分为均等份了
        } finally {
            head.unlock();
        }
    }

    /**
     * Free a subpage or a run of pages When a subpage is freed from PoolSubpage, it might be added back to subpage pool
     * of the owning PoolArena. If the subpage pool in PoolArena has at least one other PoolSubpage of given elemSize,
     * we can completely free the owning Page so it is available for subsequent allocations
     *
     * @param handle handle to free
     */
    void free(long handle, int normCapacity, ByteBuffer nioBuffer) {
        int runSize = runSize(pageShifts, handle);
        // 1.如果此run划分了subpage，则先释放subpage的elem
        // 从run handle判断是否划分为subpage
        if (isSubpage(handle)) {
            int sizeIdx = arena.size2SizeIdx(normCapacity);
            // 1.1 从Arena的subpage池数组找到该大小对应的head节点
            // smallSubpagePools[sizeIdx]
            PoolSubpage<T> head = arena.findSubpagePoolHead(sizeIdx);

            // 1.2  根据runOffset从数组中取出待释放的subpage
            int sIdx = runOffset(handle);// 从handle中得到runOffset
            PoolSubpage<T> subpage = subpages[sIdx];

            // Obtain the head of the PoolSubPage pool that is owned by the PoolArena and synchronize on it.
            // This is need as we may add it back and so alter the linked-list structure.
            head.lock();
            try {
                assert subpage != null && subpage.doNotDestroy;
                // 1.3 调用subpage.free()释放bitmapIdx指定的subpage的某elem
                // 返回true表示subpage尚在使用，不释放该subpage的run，直接返回
                // 返回false则表示该subpage内所有elem都未使用，则需要释放该subpage的run
                if (subpage.free(head, bitmapIdx(handle))) {
                    return;
                }
                assert !subpage.doNotDestroy;
                // Null out slot in the array as it was freed and we should not use it anymore.
                subpages[sIdx] = null;
            } finally {
                head.unlock();
            }
        }

        // 2.释放此run
        runsAvailLock.lock();
        try {
            // 2.1 合并前后相邻runs, 成功合并的run将从runsAvails和runsAvailMap移除
            long finalRun = collapseRuns(handle);

            // 2.2 设置Run句柄为未使用
            finalRun &= ~(1L << IS_USED_SHIFT);
            // 如果此run为Subpage，设置其句柄为非subpage
            finalRun &= ~(1L << IS_SUBPAGE_SHIFT);
            // 2.3 将合并后run插入到runsAvails中
            insertAvailRun(runOffset(finalRun), runPages(finalRun), finalRun);
            freeBytes += runSize;
        } finally {
            runsAvailLock.unlock();
        }

        if (nioBuffer != null && cachedNioBuffers != null &&
                cachedNioBuffers.size() < PooledByteBufAllocator.DEFAULT_MAX_CACHED_BYTEBUFFERS_PER_CHUNK) {
            cachedNioBuffers.offer(nioBuffer);
        }
    }

    private long collapseRuns(long handle) {
        return collapseNext(collapsePast(handle));
    }

    private long collapsePast(long handle) {
        for (; ; ) {
            int runOffset = runOffset(handle);
            int runPages = runPages(handle);

            long pastRun = getAvailRunByOffset(runOffset - 1);
            if (pastRun == -1) {
                return handle;
            }

            int pastOffset = runOffset(pastRun);
            int pastPages = runPages(pastRun);

            //is continuous
            if (pastRun != handle && pastOffset + pastPages == runOffset) {
                //remove past run
                removeAvailRun(pastRun);
                handle = toRunHandle(pastOffset, pastPages + runPages, 0);
            } else {
                return handle;
            }
        }
    }

    private long collapseNext(long handle) {
        for (; ; ) {
            int runOffset = runOffset(handle);
            int runPages = runPages(handle);

            long nextRun = getAvailRunByOffset(runOffset + runPages);
            if (nextRun == -1) {
                return handle;
            }

            int nextOffset = runOffset(nextRun);
            int nextPages = runPages(nextRun);

            //is continuous
            if (nextRun != handle && runOffset + runPages == nextOffset) {
                //remove next run
                removeAvailRun(nextRun);
                handle = toRunHandle(runOffset, runPages + nextPages, 0);
            } else {
                return handle;
            }
        }
    }

    private static long toRunHandle(int runOffset, int runPages, int inUsed) {
        return (long) runOffset << RUN_OFFSET_SHIFT
                | (long) runPages << SIZE_SHIFT
                | (long) inUsed << IS_USED_SHIFT;
    }

    void initBuf(PooledByteBuf<T> buf, ByteBuffer nioBuffer, long handle, int reqCapacity,
                 PoolThreadCache threadCache) {
        if (isSubpage(handle)) {
            initBufWithSubpage(buf, nioBuffer, handle, reqCapacity, threadCache);
        } else {
            int maxLength = runSize(pageShifts, handle);
            buf.init(this, nioBuffer, handle, runOffset(handle) << pageShifts,
                    reqCapacity, maxLength, arena.parent.threadCache());
        }
    }

    void initBufWithSubpage(PooledByteBuf<T> buf, ByteBuffer nioBuffer, long handle, int reqCapacity,
                            PoolThreadCache threadCache) {
        int runOffset = runOffset(handle);
        int bitmapIdx = bitmapIdx(handle);

        PoolSubpage<T> s = subpages[runOffset];
        assert s.doNotDestroy;
        assert reqCapacity <= s.elemSize : reqCapacity + "<=" + s.elemSize;

        int offset = (runOffset << pageShifts) + bitmapIdx * s.elemSize;
        buf.init(this, nioBuffer, handle, offset, reqCapacity, s.elemSize, threadCache);
    }

    void incrementPinnedMemory(int delta) {
        assert delta > 0;
        pinnedBytes.add(delta);
    }

    void decrementPinnedMemory(int delta) {
        assert delta > 0;
        pinnedBytes.add(-delta);
    }

    @Override
    public int chunkSize() {
        return chunkSize;
    }

    @Override
    public int freeBytes() {
        arena.lock();
        try {
            return freeBytes;
        } finally {
            arena.unlock();
        }
    }

    public int pinnedBytes() {
        return (int) pinnedBytes.value();
    }

    @Override
    public String toString() {
        final int freeBytes;
        arena.lock();
        try {
            freeBytes = this.freeBytes;
        } finally {
            arena.unlock();
        }

        return new StringBuilder()
                .append("Chunk(")
                .append(Integer.toHexString(System.identityHashCode(this)))
                .append(": ")
                .append(usage(freeBytes))
                .append("%, ")
                .append(chunkSize - freeBytes)
                .append('/')
                .append(chunkSize)
                .append(')')
                .toString();
    }

    void destroy() {
        arena.destroyChunk(this);
    }

    static int runOffset(long handle) {
        return (int) (handle >> RUN_OFFSET_SHIFT);
    }

    static int runSize(int pageShifts, long handle) {
        return runPages(handle) << pageShifts;
    }

    static int runPages(long handle) {
        return (int) (handle >> SIZE_SHIFT & 0x7fff);
    }

    static boolean isUsed(long handle) {
        return (handle >> IS_USED_SHIFT & 1) == 1L;
    }

    static boolean isRun(long handle) {
        return !isSubpage(handle);
    }

    static boolean isSubpage(long handle) {
        return (handle >> IS_SUBPAGE_SHIFT & 1) == 1L;
    }

    static int bitmapIdx(long handle) {
        return (int) handle;
    }
}
