package com.complone.base.impl;

import com.complone.base.utils.DataUnit;

/**
 * log的格式如下
 * | crc32 | length | log type | data |
 * 其中，crc32占4 byte，length占2byte，type占1byte，这三个作为log头，共占据7byte
 */
public final class LogConstants
{
    // todo find new home for these

    public static final int BLOCK_SIZE = 32768;

    // 占7byte的log头
    public static final int HEADER_SIZE = DataUnit.INT_UNIT + DataUnit.SHORT_UNIT + DataUnit.BYTE_UNIT;

    private LogConstants()
    {
    }
}