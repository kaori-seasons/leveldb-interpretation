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

import com.complone.base.CompressionType;
import com.complone.base.db.Slices;
import com.complone.base.include.Slice;
import com.complone.base.include.SliceInput;
import com.complone.base.include.SliceOutput;

import static java.util.Objects.requireNonNull;

/**
 * BlockTrailer的Size = 5 = 1byte的type + 4bytes的crc32。
 * 该类主要提供了构建BlockTrailer的方法
 */
public class BlockTrailer
{
    public static final int ENCODED_LENGTH = 5;

    private final CompressionType compressionType;
    private final int crc32c;

    public BlockTrailer(CompressionType compressionType, int crc32c)
    {
        requireNonNull(compressionType, "compressionType is null");

        this.compressionType = compressionType;
        this.crc32c = crc32c;
    }

    public CompressionType getCompressionType()
    {
        return compressionType;
    }

    public int getCrc32c()
    {
        return crc32c;
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

        BlockTrailer that = (BlockTrailer) o;

        if (crc32c != that.crc32c) {
            return false;
        }
        if (compressionType != that.compressionType) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode()
    {
        int result = compressionType.hashCode();
        result = 31 * result + crc32c;
        return result;
    }

    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder();
        sb.append("BlockTrailer");
        sb.append("{compressionType=").append(compressionType);
        sb.append(", crc32c=0x").append(Integer.toHexString(crc32c));
        sb.append('}');
        return sb.toString();
    }

    public static BlockTrailer readBlockTrailer(Slice slice)
    {
        SliceInput sliceInput = slice.input();
        // type的值占1byte，crc32c的值占4byte
        CompressionType compressionType = CompressionType.getCompressionTypeByPersistentId(sliceInput.readUnsignedByte());
        int crc32c = sliceInput.readInt();
        return new BlockTrailer(compressionType, crc32c);
    }

    public static Slice writeBlockTrailer(BlockTrailer blockTrailer)
    {
        Slice slice = Slices.allocate(ENCODED_LENGTH);
        writeBlockTrailer(blockTrailer, slice.output());
        return slice;
    }

    public static void writeBlockTrailer(BlockTrailer blockTrailer, SliceOutput sliceOutput)
    {
        sliceOutput.writeByte(blockTrailer.getCompressionType().persistentId());
        sliceOutput.writeInt(blockTrailer.getCrc32c());
    }
}