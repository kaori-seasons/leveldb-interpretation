package com.complone.base.table;

import com.complone.base.CompressionType;
import com.complone.base.db.Slices;
import com.complone.base.include.Slice;
import com.complone.base.utils.Closeables;
import com.complone.base.utils.Snappy;
import com.complone.base.utils.ByteBufferSupport;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Comparator;
import java.util.concurrent.Callable;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * MMapTable继承自Table，实现了父类的抽象函数，通过内存映射文件读取文件内容
 * 实现时根据指定的偏移和大小，读取filter的功能，对应于源码中的Table::ReadFilter()
 */
public class MMapTable
        extends Table
{
    private MappedByteBuffer data;

    public MMapTable(String name, FileChannel fileChannel, Comparator<Slice> comparator, boolean verifyChecksums)
            throws IOException
    {
        super(name, fileChannel, comparator, verifyChecksums);
        checkArgument(fileChannel.size() <= Integer.MAX_VALUE, "File must be smaller than %s bytes", Integer.MAX_VALUE);
    }

    @Override
    protected Footer init()
            throws IOException
    {
        long size = fileChannel.size();
        data = fileChannel.map(FileChannel.MapMode.READ_ONLY, 0, size);
        // 从文件的结尾读取Footer，并Decode到Footer对象中
        Slice footerSlice = Slices.copiedBuffer(data, (int) size - Footer.ENCODED_LENGTH, Footer.ENCODED_LENGTH);
        return Footer.readFooter(footerSlice);
    }

    @Override
    public Callable<?> closer()
    {
        return new Closer(name, fileChannel, data);
    }

    private static class Closer
            implements Callable<Void>
    {
        private final String name;
        private final Closeable closeable;
        private final MappedByteBuffer data;

        public Closer(String name, Closeable closeable, MappedByteBuffer data)
        {
            this.name = name;
            this.closeable = closeable;
            this.data = data;
        }

        public Void call()
        {
            ByteBufferSupport.unmap(data);
            Closeables.closeQuietly(closeable);
            return null;
        }
    }

    // 压缩block的data，返回压缩后的block
    @Override
    protected Block readBlock(BlockHandle blockHandle)
            throws IOException
    {
        // 读 block trailer，获得压缩类型 和 crc32
        BlockTrailer blockTrailer = BlockTrailer.readBlockTrailer(Slices.copiedBuffer(this.data,
                (int) blockHandle.getOffset() + blockHandle.getDataSize(),
                BlockTrailer.ENCODED_LENGTH));


        Slice uncompressedData;
        // 读取未压缩的data
        ByteBuffer uncompressedBuffer = read(this.data, (int) blockHandle.getOffset(), blockHandle.getDataSize());
        if (blockTrailer.getCompressionType() == CompressionType.SNAPPY) {
            synchronized (MMapTable.class) {
                int uncompressedLength = uncompressedLength(uncompressedBuffer);
                if (uncompressedScratch.capacity() < uncompressedLength) {
                    uncompressedScratch = ByteBuffer.allocateDirect(uncompressedLength);
                }
                uncompressedScratch.clear();

                Snappy.uncompress(uncompressedBuffer, uncompressedScratch);
                uncompressedData = Slices.copiedBuffer(uncompressedScratch);
            }
        }
        else {
            uncompressedData = Slices.copiedBuffer(uncompressedBuffer);
        }

        return new Block(uncompressedData, comparator);
    }

    public static ByteBuffer read(MappedByteBuffer data, int offset, int length)
            throws IOException
    {
        int newPosition = data.position() + offset;
        ByteBuffer block = (ByteBuffer) data.duplicate().order(ByteOrder.LITTLE_ENDIAN).clear().limit(newPosition + length).position(newPosition);
        return block;
    }
}
