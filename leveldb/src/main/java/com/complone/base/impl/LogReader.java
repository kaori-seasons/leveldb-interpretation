package com.complone.base.impl;

import com.complone.base.include.DynamicSliceOutput;
import com.complone.base.include.Slice;
import com.complone.base.include.SliceInput;
import com.complone.base.include.SliceOutput;
import com.complone.base.db.Slices;

import java.io.IOException;
import java.nio.channels.FileChannel;

import static com.complone.base.impl.Logs.getCrc32C;

/**
 * 通过给定的文件file对象，从这个file里面读出log records
 * 循环{
 *     1. 读文件的block size个字节，即一条log
 *     2. 将读到的内容放入currentBlock
 *     3. 解析block的header，将data写到currentLog中
 *     4. 根据log type决定是否要继续下次循环，知道获得完整的log record并返回
 * }
 */
public class LogReader
{
    private final FileChannel fileChannel;
    /**
     * 汇报错误
     */
    private final LogMonitor reporter;
    /**
     * 检查checksum，检查是否有损坏
     */
    private final boolean verifyChecksums;

    /**
     * 偏移，从哪里开始读取第一条record
     */
    private final long initialOffset;

    /**
     * 上次Read()返回长度 < kBlockSize，暗示到了文件结尾EOF
     */
    private boolean eof;

    /**
     * 函数ReadRecord返回的上一个record的偏移
     */
    private long lastRecordOffset;

    /**
     * 当前的读取偏移
     */
    private long endOfBufferOffset;

    /**
     * Scratch buffer in which the next record is assembled.
     */
    private final DynamicSliceOutput recordScratch = new DynamicSliceOutput(LogConstants.BLOCK_SIZE);

    /**
     * 一个block大小的缓存，用于从文件中读一个完整的block，并缓存到blockScratch
     */
    private final SliceOutput blockScratch = Slices.allocate(LogConstants.BLOCK_SIZE).output();

    /**
     * 当前读到的file中的一条record，大小为block
     */
    private SliceInput currentBlock = Slices.EMPTY_SLICE.input();

    /**
     * 当前currentBlock的data写入currentLog
     */
    private Slice currentLog = Slices.EMPTY_SLICE;

    public LogReader(FileChannel fileChannel, LogMonitor reporter, boolean verifyChecksums, long initialOffset)
    {
        this.fileChannel = fileChannel;
        this.reporter = reporter;
        this.verifyChecksums = verifyChecksums;
        this.initialOffset = initialOffset;
    }

    public long getLastRecordOffset()
    {
        return lastRecordOffset;
    }

    /**
     * 跳转到record所在的起始block
     *
     * @return true on success.
     */
    private boolean skipToInitialBlock()
    {
        // 计算在block内的偏移位置，并调整到开始读取block的起始位置
        // 开始读取日志的时候都要保证读取的是完整的block，这就是调整的目的。
        int offsetInBlock = (int) (initialOffset % LogConstants.BLOCK_SIZE);
        long blockStartLocation = initialOffset - offsetInBlock;

        // 如果偏移在最后的6byte里，肯定不是一条完整的记录，跳到下一个block
        // 可能的情况是一个record的数据区、一个record的header、空串
        if (offsetInBlock > LogConstants.BLOCK_SIZE - 6) {
            blockStartLocation += LogConstants.BLOCK_SIZE;
        }
        // 设置读取偏移，这里设置的是块的开始地址
        endOfBufferOffset = blockStartLocation;

        // 跳到包含record的第一个block里
        if (blockStartLocation > 0) {
            try {
                fileChannel.position(blockStartLocation);
            }
            catch (IOException e) {
                reportDrop(blockStartLocation, e);
                return false;
            }
        }

        return true;
    }

    public Slice readRecord()
    {
        recordScratch.reset();

        // 如果上一个record的偏移 < 初始偏移，也就是说初始偏移已经超出了几个block，那么需要把这几个block跳掉，也就是重置起始偏移
        if (lastRecordOffset < initialOffset) {
            if (!skipToInitialBlock()) {
                return null;
            }
        }

        // 我们正在读取的逻辑record的偏移
        long prospectiveRecordOffset = 0;
        // 当前是否在fragment内，也就是遇到了FIRST 类型的record
        boolean inFragmentedRecord = false;
        while (true) {
            // physicalRecordOffset存储的是当前正在读取的record的偏移值
            long physicalRecordOffset = endOfBufferOffset - currentLog.length();
            // 根据不同的type类型，分别进行处理
            LogType logType = readNextLogType();
            switch (logType) {
                // 表明是一条完整的log record，成功返回读取的user record数据
                case FULL:
                    if (inFragmentedRecord) {
                        reportCorruption(recordScratch.size(), "Partial record without end");

                    }
                    // 清空scratch，读取成功不需要返回scratch数据
                    recordScratch.reset();
                    prospectiveRecordOffset = physicalRecordOffset;
                    // 更新lastRecordOffset
                    lastRecordOffset = prospectiveRecordOffset;
                    return currentLog.copySlice();

                case FIRST:
                    if (inFragmentedRecord) {
                        reportCorruption(recordScratch.size(), "Partial record without end");
                        // 清空scratch，读取成功不需要返回scratch数据
                        recordScratch.reset();
                    }
                    prospectiveRecordOffset = physicalRecordOffset;
                    recordScratch.writeBytes(currentLog);
                    inFragmentedRecord = true;
                    break;

                case MIDDLE:
                    if (!inFragmentedRecord) {
                        reportCorruption(recordScratch.size(), "Missing start of fragmented record");

                        // 清空scratch，读取成功不需要返回scratch数据
                        recordScratch.reset();
                    }
                    else {
                        recordScratch.writeBytes(currentLog);
                    }
                    break;

                case LAST:
                    if (!inFragmentedRecord) {
                        reportCorruption(recordScratch.size(), "Missing start of fragmented record");

                        // 清空scratch，读取成功不需要返回scratch数据
                        recordScratch.reset();
                    }
                    else {
                        recordScratch.writeBytes(currentLog);
                        lastRecordOffset = prospectiveRecordOffset;
                        // 最后一个log，返回data
                        return recordScratch.slice().copySlice();
                    }
                    break;

                case EOF:
                    if (inFragmentedRecord) {
                        reportCorruption(recordScratch.size(), "Partial record without end");

                        // clear the scratch and return
                        recordScratch.reset();
                    }
                    return null;

                case BAD_CHUNK:
                    if (inFragmentedRecord) {
                        reportCorruption(recordScratch.size(), "Error in middle of record");
                        inFragmentedRecord = false;
                        recordScratch.reset();
                    }
                    break;

                default:
                    int dropSize = currentLog.length();
                    if (inFragmentedRecord) {
                        dropSize += recordScratch.size();
                    }
                    reportCorruption(dropSize, String.format("Unexpected chunk type %s", logType));
                    inFragmentedRecord = false;
                    recordScratch.reset();
                    break;
            }
        }
    }

    /**
     * 读取record当中的data到currentLog中，返回logType
     */
    private LogType readNextLogType()
    {
        // 清楚当前log内容
        currentLog = Slices.EMPTY_SLICE;

        // 如果buffer小于blockheader的size
        if (currentBlock.available() < LogConstants.HEADER_SIZE) {
            if (!readNextBlock()) {
                if (eof) {
                    return LogType.EOF;
                }
            }
        }

        // 解析出log header
        // 根据log的格式，前4 byte是crc32，后2 byte是长度，第7byte是type
        int expectedChecksum = currentBlock.readInt();
        int length = currentBlock.readUnsignedByte();
        length = length | currentBlock.readUnsignedByte() << 8;
        byte logTypeId = currentBlock.readByte();
        LogType logType = LogType.getLogChunkTypeByPersistentId(logTypeId);

        // 如果长度超出block长度，汇报超出错误，情况currentBlock，返回BAD_CHUNK
        if (length > currentBlock.available()) {
            int dropSize = currentBlock.available() + LogConstants.HEADER_SIZE;
            reportCorruption(dropSize, "Invalid chunk length");
            currentBlock = Slices.EMPTY_SLICE.input();
            return LogType.BAD_CHUNK;
        }

        // 对于ZERO_TYPE，跳过record，不报错误，此种情况是在写的过程中由代码产生的
        if (logType == LogType.ZERO_TYPE && length == 0) {
            currentBlock = Slices.EMPTY_SLICE.input();
            return LogType.BAD_CHUNK;
        }

        // 如果record的开始位置在initialOffset之前，则跳过，并返回BAD_CHUNK
        if (endOfBufferOffset - LogConstants.HEADER_SIZE - length < initialOffset) {
            currentBlock.skipBytes(length);
            return LogType.BAD_CHUNK;
        }

        // 读取当前log
        currentLog = currentBlock.readBytes(length);
        // 校验CRC32，如果校验出错，则汇报错误，并返回kBadRecord。
        if (verifyChecksums) {
            int actualChecksum = getCrc32C(logTypeId, currentLog);
            if (actualChecksum != expectedChecksum) {
                int dropSize = currentBlock.available() + LogConstants.HEADER_SIZE;
                currentBlock = Slices.EMPTY_SLICE.input();
                reportCorruption(dropSize, "Invalid chunk checksum");
                return LogType.BAD_CHUNK;
            }
        }

        // 跳过unknown type
        if (logType == LogType.UNKNOWN) {
            reportCorruption(length, String.format("Unknown chunk type %d", logType.getPersistentId()));
            return LogType.BAD_CHUNK;
        }
        // 返回logtype
        return logType;
    }

    /**
     * 从文件读取一个block的数据，赋值给currentBlock，更新endOfBufferOffset，返回该block是否可读
     * @return 当前block是否可读
     */
    public boolean readNextBlock()
    {
        // eof为false，表明还没有到文件结尾，清空buffer，并读取数据
        if (eof) {
            return false;
        }

        // 清空buffer，因为上次肯定读了一个完整的record
        blockScratch.reset();

        // 读下一个完整的block
        while (blockScratch.writableBytes() > 0) {
            try {
                int bytesRead = blockScratch.writeBytes(fileChannel, blockScratch.writableBytes());
                if (bytesRead < 0) {
                    // 如果没有可读的内容，说明已经到文件末尾了
                    eof = true;
                    break;
                }
                // 更新buffer读取偏移值
                endOfBufferOffset += bytesRead;
            }
            catch (IOException e) {
                // 读取失败，currentBlock置空，返回错误报告，设置eof为true，返回不可读
                currentBlock = Slices.EMPTY_SLICE.input();
                reportDrop(LogConstants.BLOCK_SIZE, e);
                eof = true;
                return false;
            }

        }
        // 将缓存中的数据赋值给currentBlock
        currentBlock = blockScratch.slice().input();
        // 返回下一个block可读
        return currentBlock.isReadable();
    }

    /**
     * 报告
     */
    private void reportCorruption(long bytes, String reason)
    {
        if (reporter != null) {
            reporter.corruption(bytes, reason);
        }
    }

    /**
     * 报告
     */
    private void reportDrop(long bytes, Throwable reason)
    {
        if (reporter != null) {
            reporter.corruption(bytes, reason);
        }
    }
}