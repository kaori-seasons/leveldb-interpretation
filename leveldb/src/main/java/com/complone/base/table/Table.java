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

import com.complone.base.impl.SeekingIterable;
import com.complone.base.include.Slice;
import com.complone.base.utils.Closeables;
import com.complone.base.utils.Coding;
import com.complone.base.utils.TableIterator;
import com.google.common.base.Throwables;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Comparator;
import java.util.concurrent.Callable;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public abstract class Table
        implements SeekingIterable<Slice, Slice>
{
    protected final String name;
    protected final FileChannel fileChannel;
    protected final Comparator<Slice> comparator;
    protected final boolean verifyChecksums;
    protected final Block indexBlock;
    protected final BlockHandle metaindexBlockHandle;

    public Table(String name, FileChannel fileChannel, Comparator<Slice> comparator, boolean verifyChecksums)
            throws IOException
    {
        requireNonNull(name, "name is null");
        requireNonNull(fileChannel, "fileChannel is null");
        long size = fileChannel.size();
        checkArgument(size >= Footer.ENCODED_LENGTH,
                "File is corrupt: size must be at least %s bytes", Footer.ENCODED_LENGTH);
        requireNonNull(comparator, "comparator is null");

        this.name = name;
        this.fileChannel = fileChannel;
        this.verifyChecksums = verifyChecksums;
        this.comparator = comparator;

        // Footer的成员metaindexBlockHandle指出了meta index block的起始位置和大小；
        // Footer的成员indexBlockHandle指出了index block的起始地址和大小；
        Footer footer = init();
        indexBlock = readBlock(footer.getIndexBlockHandle());
        metaindexBlockHandle = footer.getMetaindexBlockHandle();
    }

    protected abstract Footer init()
            throws IOException;

    @Override
    public TableIterator iterator()
    {
        return new TableIterator(this, indexBlock.iterator());
    }
    // 根据blockEntry的成员变量访问table中对应的Block对象
    public Block openBlock(Slice blockEntry)
    {
        BlockHandle blockHandle = BlockHandle.readBlockHandle(blockEntry.input());
        Block dataBlock;
        try {
            dataBlock = readBlock(blockHandle);
        }
        catch (IOException e) {
            throw Throwables.propagate(e);
        }
        return dataBlock;
    }

    protected static ByteBuffer uncompressedScratch = ByteBuffer.allocateDirect(4 * 1024 * 1024);
    // 根据BlockHandle读取Block
    protected abstract Block readBlock(BlockHandle blockHandle)
            throws IOException;

    protected int uncompressedLength(ByteBuffer data)
            throws IOException
    {
        int length = Coding.decodeInt(data.duplicate());
        return length;
    }

    /**
     * 给定一个key，返回一个data block在file中的偏移量
     */
    public long getApproximateOffsetOf(Slice key)
    {
        // Index block是对Data Block的索引，对于其中的每个记录，其key >= Data Block最后一条记录的key，
        // 同时 < 其后Data Block的第一条记录的key；value是指向data index的BlockHandle；
        BlockIterator iterator = indexBlock.iterator();
        iterator.seek(key);
        if (iterator.hasNext()) {
            BlockHandle blockHandle = BlockHandle.readBlockHandle(iterator.next().getValue().input());
            return blockHandle.getOffset();
        }
        // 如果key比该文件最后一个key都大，那么返回metaindex的偏移量
        return metaindexBlockHandle.getOffset();
    }

    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder();
        sb.append("Table");
        sb.append("{name='").append(name).append('\'');
        sb.append(", comparator=").append(comparator);
        sb.append(", verifyChecksums=").append(verifyChecksums);
        sb.append('}');
        return sb.toString();
    }

    public Callable<?> closer()
    {
        return new Closer(fileChannel);
    }

    private static class Closer
            implements Callable<Void>
    {
        private final Closeable closeable;

        public Closer(Closeable closeable)
        {
            this.closeable = closeable;
        }

        @Override
        public Void call()
        {
            Closeables.closeQuietly(closeable);
            return null;
        }
    }
}
