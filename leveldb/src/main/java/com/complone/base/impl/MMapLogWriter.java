package com.complone.base.impl;

import com.complone.base.include.Slice;
import com.complone.base.include.SliceInput;
import com.complone.base.utils.Closeables;
import com.complone.base.db.Slices;
import com.complone.base.include.SliceOutput;
import com.complone.base.utils.ByteBufferSupport;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.complone.base.impl.Logs.getCrc32C;
import static java.util.Objects.requireNonNull;

public class MMapLogWriter
        implements LogWriter
{
    private static final int PAGE_SIZE = 1024 * 1024;

    private final File file;
    private final long fileNumber;
    private final FileChannel fileChannel;
    private final AtomicBoolean closed = new AtomicBoolean();
    private MappedByteBuffer mappedByteBuffer;
    private long fileOffset;
    /**
     * 在block中的偏移量
     */
    private int blockOffset;

    public MMapLogWriter(File file, long fileNumber)
            throws IOException
    {
        requireNonNull(file, "file is null");
        checkArgument(fileNumber >= 0, "fileNumber is negative");
        this.file = file;
        this.fileNumber = fileNumber;
        this.fileChannel = new RandomAccessFile(file, "rw").getChannel();
        // 这里使用了内存映射文件，内存映射文件的读写效率比普通文件要高。
        // 内存映射实现将一个文件或者文件的一部分"映射"到内存中。然后，这个文件就可以当作是内存数组来访问，因此效率高
        mappedByteBuffer = fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, PAGE_SIZE);
    }

    @Override
    public boolean isClosed()
    {
        return closed.get();
    }

    @Override
    public synchronized void close()
            throws IOException
    {
        closed.set(true);

        destroyMappedByteBuffer();
        // truncate()方法截取一个文件。截取文件时，文件将中指定长度后面的部分将被删除。
        if (fileChannel.isOpen()) {
            fileChannel.truncate(fileOffset);
        }

        // 关闭通道
        Closeables.closeQuietly(fileChannel);
    }

    @Override
    public synchronized void delete()
            throws IOException
    {
        close();

        // 删除文件
        file.delete();
    }

    // 更新文件偏移，释放mappedByteBuffer句柄
    private void destroyMappedByteBuffer()
    {
        if (mappedByteBuffer != null) {
            fileOffset += mappedByteBuffer.position();
            unmap();
        }
        mappedByteBuffer = null;
    }

    @Override
    public File getFile()
    {
        return file;
    }

    @Override
    public long getFileNumber()
    {
        return fileNumber;
    }

    @Override
    public synchronized void addRecord(Slice record, boolean force)
            throws IOException
    {
        checkState(!closed.get(), "Log has been closed");

        SliceInput sliceInput = record.input();

        // 初始化begin=true，表明是第一条log record
        // 用来跟踪判断log在first、 middle、 last block里
        boolean begin = true;


        // 进入一个do{}while循环，直到写入出错，或者成功写入全部数据
        do {
            // 计算block剩余大小，以及本次log record可写入数据长度
            int bytesRemainingInBlock = LogConstants.BLOCK_SIZE - blockOffset;
            checkState(bytesRemainingInBlock >= 0);

            // 首先查看当前block是否<7，如果<7则补位，并重置block偏移。
            if (bytesRemainingInBlock < LogConstants.HEADER_SIZE) {
                if (bytesRemainingInBlock > 0) {
                    // 将前7位补0
                    ensureCapacity(bytesRemainingInBlock);
                    mappedByteBuffer.put(new byte[bytesRemainingInBlock]);
                }
                blockOffset = 0;
                // 计算block剩余大小
                bytesRemainingInBlock = LogConstants.BLOCK_SIZE - blockOffset;
            }

            // 如果剩余可写空间不足7，报错
            int bytesAvailableInBlock = bytesRemainingInBlock - LogConstants.HEADER_SIZE;
            checkState(bytesAvailableInBlock >= 0);

            boolean end;
            // 本次log record可写入数据长度
            int fragmentLength;
            if (sliceInput.available() > bytesAvailableInBlock) {
                end = false;
                fragmentLength = bytesAvailableInBlock;
            }
            else {
                end = true;
                fragmentLength = sliceInput.available();
            }

            // 根据两个值，判断log type
            LogType type;
            // 两者相等，表明写完
            if (begin && end) {
                type = LogType.FULL;
            }
            else if (begin) {
                type = LogType.FIRST;
            }
            else if (end) {
                type = LogType.LAST;
            }
            else {
                type = LogType.MIDDLE;
            }

            // 写入log
            writeChunk(type, sliceInput.readBytes(fragmentLength));

            // 已经不在第一个block了，更新begin标记
            begin = false;
        } while (sliceInput.isReadable());

        if (force) {
            // 强制将此缓冲区上的任何更改写入映射到永久磁盘存储器上。
            mappedByteBuffer.force();
        }
    }

    private void writeChunk(LogType type, Slice slice)
            throws IOException
    {
        checkArgument(slice.length() <= 0xffff, "length %s is larger than two bytes", slice.length());
        checkArgument(blockOffset + LogConstants.HEADER_SIZE <= LogConstants.BLOCK_SIZE);

        // 新建header
        Slice header = newLogRecordHeader(type, slice);

        // 将header和data写入log
        ensureCapacity(header.length() + slice.length());
        header.getBytes(0, mappedByteBuffer);
        slice.getBytes(0, mappedByteBuffer);
        // 更新block偏移
        blockOffset += LogConstants.HEADER_SIZE + slice.length();
    }

    /**
     * 如果小于bytes位，重新分配mappedByteBuffer
     */
    private void ensureCapacity(int bytes)
            throws IOException
    {
        if (mappedByteBuffer.remaining() < bytes) {
            // remap
            fileOffset += mappedByteBuffer.position();
            unmap();

            mappedByteBuffer = fileChannel.map(FileChannel.MapMode.READ_WRITE, fileOffset, PAGE_SIZE);
        }
    }

    private void unmap()
    {
        ByteBufferSupport.unmap(mappedByteBuffer);
    }

    private static Slice newLogRecordHeader(LogType type, Slice slice)
    {
        int crc = getCrc32C(type.getPersistentId(), slice.getData(), slice.getOffset(), slice.length());

        // 构建log头
        Slice header = Slices.allocate(LogConstants.HEADER_SIZE);
        SliceOutput sliceOutput = header.output();
        sliceOutput.writeInt(crc);
        sliceOutput.writeByte((byte) (slice.length() & 0xff));
        sliceOutput.writeByte((byte) (slice.length() >>> 8));
        sliceOutput.writeByte((byte) (type.getPersistentId()));

        return header;
    }
}