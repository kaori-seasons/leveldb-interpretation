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

import com.complone.base.include.Slice;
import com.complone.base.include.SliceInput;
import com.complone.base.utils.Coding;
import com.complone.base.db.Slices;
import com.complone.base.include.SliceOutput;

/**
 * BlockHandle是一个结构体，成员offset是Block在文件中的偏移，成员dataSize是block的大小
 */
public class BlockHandle
{
    public static final int MAX_ENCODED_LENGTH = 10 + 10;

    private final long offset;
    private final int dataSize;

    BlockHandle(long offset, int dataSize)
    {
        this.offset = offset;
        this.dataSize = dataSize;
    }

    public long getOffset()
    {
        return offset;
    }

    public int getDataSize()
    {
        return dataSize;
    }

    /**
     *
     * @return 一个完整的Block的长度，包括Block数据的长度 + BlockTrailer的长度
     */
    public int getFullBlockSize()
    {
        return dataSize + BlockTrailer.ENCODED_LENGTH;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        BlockHandle that = (BlockHandle) o;

        if (dataSize != that.dataSize) {
            return false;
        }
        if (offset != that.offset) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode()
    {
        int result = (int) (offset ^ (offset >>> 32));
        result = 31 * result + dataSize;
        return result;
    }

    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder();
        sb.append("BlockHandle");
        sb.append("{offset=").append(offset);
        sb.append(", dataSize=").append(dataSize);
        sb.append('}');
        return sb.toString();
    }

    public static BlockHandle readBlockHandle(SliceInput sliceInput)
    {
        long offset = Coding.decodeLong(sliceInput);
        long size = Coding.decodeLong(sliceInput);

        if (size > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("Blocks can not be larger than Integer.MAX_VALUE");
        }

        return new BlockHandle(offset, (int) size);
    }

    public static Slice writeBlockHandle(BlockHandle blockHandle)
    {
        Slice slice = Slices.allocate(MAX_ENCODED_LENGTH);
        SliceOutput sliceOutput = slice.output();
        writeBlockHandleTo(blockHandle, sliceOutput);
        return slice.slice();
    }

    public static void writeBlockHandleTo(BlockHandle blockHandle, SliceOutput sliceOutput)
    {
        Coding.encodeLong(blockHandle.offset, sliceOutput);
        Coding.encodeLong(blockHandle.dataSize, sliceOutput);
    }
}
