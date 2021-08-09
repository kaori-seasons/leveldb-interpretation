package com.complone.base.impl;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * log中的type，下面是源码中对于log格式的描述
 * The log file contents are a sequence of 32KB blocks.
 * The only exception is that the tail of the file may contain a partial block.
 * Each block consists of a sequence of records:
 *     block:= record* trailer?
 *     record :=
 *     checksum: uint32    // crc32c of type and data[] ; little-endian
 *     length: uint16       // little-endian
 *     type: uint8          // One of FULL,FIRST, MIDDLE, LAST
 *     data: uint8[length]
 * A record never starts within the last six bytes of a block (since it won't fit).
 * Any leftover bytes here form the trailer, which must consist entirely of zero bytes and must be skipped by readers.
 *
 * Log Type有4种：FULL = 1、FIRST = 2、MIDDLE = 3、LAST = 4。
 * FULL类型表明该log record包含了完整的user record；
 * 而user record可能内容很多，超过了block的可用大小，就需要分成几条log record，第一条类型为FIRST，中间的为MIDDLE，最后一条为LAST。
 * 也就是：
 * 1. FULL，说明该log record包含一个完整的user record
 * 2. FIRST，说明是user record的第一条log record
 * 3. MIDDLE，说明是user record中间的log record
 * 4. LAST，说明是user record最后的一条log record
 */
public enum LogType
{
    ZERO_TYPE(0),
    FULL(1),
    FIRST(2),
    MIDDLE(3),
    LAST(4),
    EOF,
    BAD_CHUNK,
    UNKNOWN;

    public static LogType getLogChunkTypeByPersistentId(int persistentId)
    {
        for (LogType logType : LogType.values()) {
            if (logType.persistentId != null && logType.persistentId == persistentId) {
                return logType;
            }
        }
        return UNKNOWN;
    }

    private final Integer persistentId;

    LogType()
    {
        this.persistentId = null;
    }

    LogType(int persistentId)
    {
        this.persistentId = persistentId;
    }

    public int getPersistentId()
    {
        checkArgument(persistentId != null, "%s is not a persistent chunk type", name());
        return persistentId;
    }
}
