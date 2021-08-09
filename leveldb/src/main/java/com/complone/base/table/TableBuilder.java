/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.complone.base.table;


import com.complone.base.CompressionType;
import com.complone.base.Options;
import com.complone.base.include.Slice;
import com.complone.base.utils.Snappy;
import com.google.common.base.Throwables;
import com.complone.base.db.Slices;
import com.complone.base.utils.Crc32;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;
import static com.complone.base.impl.VersionSet.TARGET_FILE_SIZE;
public class TableBuilder
{
    public static final long TABLE_MAGIC_NUMBER = 0xdb4775248b80fb57L;

    private final int blockRestartInterval;
    private final int blockSize;
    private final CompressionType compressionType;
    // table文件
    private final FileChannel fileChannel;
    // table的data block
    private final BlockBuilder dataBlockBuilder;
    // table的index block
    private final BlockBuilder indexBlockBuilder;
    // 当前data block最后的k/v对的key
    private Slice lastKey;
    private final UserComparator userComparator;
    // 当前data block的个数，初始0
    private long entryCount;
    // 调用 Finish() 或 Abandon()之后，closed会被置为true，构建table结束
    private boolean closed;

    /* 直到遇到下一个databock的第一个key时，我们才为上一个datablock生成index entry
     * 这样的好处是：可以为index使用较短的key；比如上一个data block最后一个k/v的key是"the quick brown fox"，
     * 其后继data block的第一个key是"the who"，我们就可以用一个较短的字符串"the r"作为上一个data block的index block entry的key。
     * 简而言之，就是在开始下一个datablock时，Leveldb才将上一个data block加入到index block中。
     * 标记pendingIndexEntry就是干这个用的，对应data block的index entry信息就保存在（BlockHandle）pendingHandle。
     */
    private boolean pendingIndexEntry;
    // 添加到index block的data block的信息
    private BlockHandle pendingHandle;
    // 压缩后的data block，临时存储，写入后即被清空
    private Slice compressedOutput;
    // 要写入data block在table文件中的偏移
    private long position;
    // data block的选项
    public TableBuilder(Options options, FileChannel fileChannel, UserComparator userComparator)
    {
        requireNonNull(options, "options is null");
        requireNonNull(fileChannel, "fileChannel is null");
        try {
            checkState(position == fileChannel.position(),
                    "Expected position %s to equal fileChannel.position %s",
                    position, fileChannel.position());
        }
        catch (IOException e) {
            throw Throwables.propagate(e);
        }

        this.fileChannel = fileChannel;
        this.userComparator = userComparator;

        blockRestartInterval = options.blockRestartInterval();
        blockSize = options.blockSize();
        compressionType = options.compressionType();
        // 这里根据用户传入的comparator来进行比较
        dataBlockBuilder = new BlockBuilder((int) Math.min(blockSize * 1.1, TARGET_FILE_SIZE), blockRestartInterval, userComparator);

        // with expected 50% compression
        int expectedNumberOfBlocks = 1024;
        indexBlockBuilder = new BlockBuilder(BlockHandle.MAX_ENCODED_LENGTH * expectedNumberOfBlocks, 1, userComparator);
        // 初始化最后的key是空Slice
        lastKey = Slices.EMPTY_SLICE;
    }

    public long getEntryCount()
    {
        return entryCount;
    }

    public long getFileSize()
            throws IOException
    {
        return position + dataBlockBuilder.currentSizeEstimate();
    }

    public void add(BlockEntry blockEntry)
            throws IOException
    {
        requireNonNull(blockEntry, "blockEntry is null");
        add(blockEntry.getKey(), blockEntry.getValue());
    }

    public void add(Slice key, Slice value)
            throws IOException
    {
        requireNonNull(key, "key is null");
        requireNonNull(value, "value is null");
        // 首先保证文件没有close，也就是没有调用过Finish/Abandon
        checkState(!closed, "table is finished");

        if (entryCount > 0) {
            assert (userComparator.compare(key, lastKey) > 0) : "key must be greater than last key";
        }

        // 如果标记pendingIndexEntry为true，表明遇到下一个data block的第一个k/v，
        // 通过Comparator的FindShortestSeparator为上一个data block生成一个最短的key，写入index block中。
        if (pendingIndexEntry) {
            checkState(dataBlockBuilder.isEmpty(), "Internal error: Table has a pending index entry but data block builder is empty");
            // 找到一个介于lastkey和key之间的最短字符串，这样做是为了尽量压缩index block的空间
            Slice shortestSeparator = userComparator.findShortestSeparator(lastKey, key);

            Slice handleEncoding = BlockHandle.writeBlockHandle(pendingHandle);
            indexBlockBuilder.add(shortestSeparator, handleEncoding);
            pendingIndexEntry = false;
        }

        lastKey = key;
        entryCount++;
        dataBlockBuilder.add(key, value);

        int estimatedBlockSize = dataBlockBuilder.currentSizeEstimate();
        // data block的个数超过限制，就立刻Flush到文件中。
        if (estimatedBlockSize >= blockSize) {
            flush();
        }
    }

    private void flush()
            throws IOException
    {
        // 保证文件没有关闭
        checkState(!closed, "table is finished");
        // 如果data block是空，则直接返回
        if (dataBlockBuilder.isEmpty()) {
            return;
        }

        checkState(!pendingIndexEntry, "Internal error: Table already has a pending index entry to flush");

        pendingHandle = writeBlock(dataBlockBuilder);
        pendingIndexEntry = true;
    }

    /**
     * 将data block写入到文件中，该函数同时还设置data block的index entry信息。
     * @param blockBuilder
     * @return
     * @throws IOException
     */
    private BlockHandle writeBlock(BlockBuilder blockBuilder)
            throws IOException
    {
        // 关闭该block，获得block的序列化数据Slice
        Slice raw = blockBuilder.finish();

        // 根据配置参数决定是否压缩，以及根据压缩格式压缩数据内容
        Slice blockContents = raw;
        CompressionType blockCompressionType = CompressionType.NONE;
        if (compressionType == CompressionType.SNAPPY) {
            // 初始化compressedOutput，即压缩后的data block的临时空间，长度为源数据的长度
            ensureCompressedOutputCapacity(maxCompressedLength(raw.length()));
            try {
                // 使用snappy压缩，将data block中的数据写入compressedOutput中
                int compressedSize = Snappy.compress(raw.getData(), raw.getOffset(), raw.length(), compressedOutput.getData(), 0);

                // 压缩率高于12.5%，压缩存储
                if (compressedSize < raw.length() - (raw.length() / 8)) {
                    blockContents = compressedOutput.slice(0, compressedSize);
                    blockCompressionType = CompressionType.SNAPPY;
                }
            }
            catch (IOException ignored) {

            }
        }

        // 构建1 byte的type和4byte的crc校验码
        BlockTrailer blockTrailer = new BlockTrailer(blockCompressionType, crc32c(blockContents, blockCompressionType));
        Slice trailer = BlockTrailer.writeBlockTrailer(blockTrailer);

        // 给这个block创建handle
        BlockHandle blockHandle = new BlockHandle(position, blockContents.length());

        // 将data block写入文件
        position += fileChannel.write(new ByteBuffer[] {blockContents.toByteBuffer(), trailer.toByteBuffer()});

        // 清空data block
        blockBuilder.reset();

        return blockHandle;
    }

    private static int maxCompressedLength(int length)
    {
        return 32 + length + (length / 6);
    }

    public void finish()
            throws IOException
    {
        checkState(!closed, "table is finished");

        // 把现有的数据刷到file中
        flush();

        // 把文件设为closed
        closed = true;

        // 通过meta index block，可以根据filter名字快速定位到filter的数据区。
        BlockBuilder metaIndexBlockBuilder = new BlockBuilder(256, blockRestartInterval, new BytewiseComparator());

        BlockHandle metaindexBlockHandle = writeBlock(metaIndexBlockBuilder);

        // 如果成功Flush过data block，那么需要为最后一块data block设置index block，并加入到index block中。
        if (pendingIndexEntry) {
            Slice shortSuccessor = userComparator.findShortSuccessor(lastKey);

            Slice handleEncoding = BlockHandle.writeBlockHandle(pendingHandle);
            indexBlockBuilder.add(shortSuccessor, handleEncoding);
            pendingIndexEntry = false;
        }

        // 写入index block
        BlockHandle indexBlockHandle = writeBlock(indexBlockBuilder);

        // 写footer
        Footer footer = new Footer(metaindexBlockHandle, indexBlockHandle);
        Slice footerEncoding = Footer.writeFooter(footer);
        position += fileChannel.write(footerEncoding.toByteBuffer());
    }

    public void abandon()
    {
        checkState(!closed, "table is finished");
        closed = true;
    }

    public static int crc32c(Slice data, CompressionType type)
    {
        Crc32 crc32c = new Crc32();
        crc32c.update(data.getData(), data.getOffset(), data.length());
        crc32c.update(type.persistentId() & 0xFF);
        return crc32c.getMaskedValue();
    }

    public void ensureCompressedOutputCapacity(int capacity)
    {
        if (compressedOutput != null && compressedOutput.length() > capacity) {
            return;
        }
        compressedOutput = Slices.allocate(capacity);
    }
}