package com.complone.base.impl;

import com.complone.base.include.Slice;
import com.complone.base.utils.Crc32;

import java.io.File;
import java.io.IOException;

public final class Logs
{
    private Logs()
    {
    }
    // 由文件创建LogWriter，构建内存映射、通道写文件两种方式
    public static LogWriter createLogWriter(File file, long fileNumber)
            throws IOException
    {
        if (LevelDBFactory.USE_MMAP) {
            return new MMapLogWriter(file, fileNumber);
        }
        else {
            return new FileChannelLogWriter(file, fileNumber);
        }
    }

    // 生产Crc32码
    public static int getCrc32C(int chunkTypeId, Slice slice)
    {
        return getCrc32C(chunkTypeId, slice.getData(), slice.getOffset(), slice.length());
    }

    public static int getCrc32C(int chunkTypeId, byte[] buffer, int offset, int length)
    {
        // Compute the crc of the record type and the payload.
        Crc32 crc32C = new Crc32();
        crc32C.update(chunkTypeId);
        crc32C.update(buffer, offset, length);
        return crc32C.getMaskedValue();
    }
}