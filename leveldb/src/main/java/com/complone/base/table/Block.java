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

import com.complone.base.db.Slices;
import com.complone.base.impl.SeekingIterable;
import com.complone.base.include.Slice;
import com.complone.base.utils.DataUnit;
import com.google.common.base.Preconditions;

import java.util.Comparator;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

/**
 * 这里的Block指的是源码中的data部分，剩余的5字节在BlockTrailer中，1字节是type（压缩类型），4字节是crc32
 * 非常好的源代码分析：https://blog.csdn.net/sparkliang/article/details/8635821
 * 原代码中对于block的存储格式在table_format中做了详细描述：
 * filter block is formatted as follows:
 *
 *     [filter 0]
 *     [filter 1]
 *     [filter 2]
 *     ...
 *     [filter N-1]
 *
 *     [offset of filter 0]                  : 4 bytes
 *     [offset of filter 1]                  : 4 bytes
 *     [offset of filter 2]                  : 4 bytes
 *     ...
 *     [offset of filter N-1]                : 4 bytes
 *
 *     [offset of beginning of offset array] : 4 bytes
 *     lg(base)                              : 1 byte
 *
 * The offset array at the end of the filter block allows efficient
 * mapping from a data block offset to the corresponding filter.
 * block分为k/v存储区和后面的重启点存储区两部分
 * 对于一个k/v对，其在block中的存储格式为：
 * 共享前缀长度         shared_bytes:    varint32
 * 前缀之后的字符串长度 unshared_bytes:  varint32
 * 值的长度             value_length:     varint32
 * 前缀之后的字符串     key_delta:        char[unshared_bytes]
 * 值                   value:           char[value_length]
 * 如下所示存储：
 * shared_bytes | unshared_bytes | value_length | key_delta | value
 */
public class Block
        implements SeekingIterable<Slice, Slice>
{
    private final Slice block;
    private final Comparator<Slice> comparator;
    // k/v存储区
    private final Slice data;
    //重启点存储区
    private final Slice restartPositions;

    public Block(Slice block, Comparator<Slice> comparator)
    {

        requireNonNull(block, "block is null");
        Preconditions.checkArgument(block.length() >= DataUnit.INT_UNIT, "Block is corrupt: size must be at least %s block", DataUnit.INT_UNIT);
        requireNonNull(comparator, "comparator is null");

        block = block.slice();
        this.block = block;
        this.comparator = comparator;

        /**
         * leveldb中的key都是经过压缩的，重启点的第一个key是被写入的一个完整的key，这些重启点都是写在文件的开头，因此在查找key的
         * 时候可以避免读整个文件。
         * 最后的四个字节是重启点个数，获取重启点个数
         */
        int restartCount = block.getInt(block.length() - DataUnit.INT_UNIT);

        if (restartCount > 0) {
            // 根据重启点的个数，计算第一个重启点的位置
            int restartOffset = block.length() - (1 + restartCount) * DataUnit.INT_UNIT;
            checkArgument(restartOffset < block.length() - DataUnit.INT_UNIT, "Block is corrupt: restart offset count is greater than block size");
            // 从传入的Slice中解析重启点存储区
            restartPositions = block.slice(restartOffset, restartCount * DataUnit.INT_UNIT);
            // 从传入的Slice中解析出k/v存储区
            data = block.slice(0, restartOffset);
        }
        else {
            data = Slices.EMPTY_SLICE;
            restartPositions = Slices.EMPTY_SLICE;
        }
    }

    public long size()
    {
        return block.length();
    }

    @Override
    public BlockIterator iterator()
    {
        return new BlockIterator(data, restartPositions, comparator);
    }
}