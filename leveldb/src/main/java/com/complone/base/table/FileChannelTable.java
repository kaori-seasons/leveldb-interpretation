package com.complone.base.table;

import com.complone.base.CompressionType;
import com.complone.base.db.Slices;
import com.complone.base.include.Slice;
import com.complone.base.utils.Snappy;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Comparator;

/**
 * FileChannelTable继承自Table，实现了父类的抽象函数，通过FileChannel读取文件内容
 * 实现时根据指定的偏移和大小，读取filter的功能，对应于源码中的Table::ReadFilter()
 */
public class FileChannelTable
        extends Table
{
    public FileChannelTable(String name, FileChannel fileChannel, Comparator<Slice> comparator, boolean verifyChecksums)
            throws IOException
    {
        super(name, fileChannel, comparator, verifyChecksums);
    }

    @Override
    protected Footer init()
            throws IOException
    {
        long size = fileChannel.size();
        // 从文件的结尾读取Footer，并Decode到Footer对象中
        ByteBuffer footerData = read(size - Footer.ENCODED_LENGTH, Footer.ENCODED_LENGTH);
        return Footer.readFooter(Slices.copiedBuffer(footerData));
    }

    // 压缩block的data，返回压缩后的block
    @Override
    protected Block readBlock(BlockHandle blockHandle)
            throws IOException
    {
        // 读 block trailer，获得压缩类型 和 crc32
        ByteBuffer trailerData = read(blockHandle.getOffset() + blockHandle.getDataSize(), BlockTrailer.ENCODED_LENGTH);
        BlockTrailer blockTrailer = BlockTrailer.readBlockTrailer(Slices.copiedBuffer(trailerData));

        // 读取未压缩的data
        ByteBuffer uncompressedBuffer = read(blockHandle.getOffset(), blockHandle.getDataSize());
        Slice uncompressedData;
        if (blockTrailer.getCompressionType() == CompressionType.SNAPPY) {
            synchronized (FileChannelTable.class) {
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

    private ByteBuffer read(long offset, int length)
            throws IOException
    {
        ByteBuffer uncompressedBuffer = ByteBuffer.allocate(length);
        fileChannel.read(uncompressedBuffer, offset);
        if (uncompressedBuffer.hasRemaining()) {
            throw new IOException("Could not read all the data");
        }
        uncompressedBuffer.clear();
        return uncompressedBuffer;
    }
}