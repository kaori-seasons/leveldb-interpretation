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
package com.complone.base.include;

import com.complone.base.db.Slices;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.ScatteringByteChannel;
import java.nio.charset.Charset;
/**
 * DynamicSliceOutput与BasicSliceOutput不同的是：当Slice容量不足时，动态扩容Slice容量
 */
public class DynamicSliceOutput
        extends SliceOutput
{
    private Slice slice;
    private int index;

    /**
     * 构造函数与BasicSliceOutput不同，BasicSliceOutput是由Slice构建，DynamicSliceOutput通过传入容量构建
     * @param estimatedSize
     */
    public DynamicSliceOutput(int estimatedSize)
    {
        this.slice = new Slice(estimatedSize);
    }

    @Override
    public void reset()
    {
        index = 0;
    }

    @Override
    public int size()
    {
        return index;
    }

    @Override
    public boolean isWritable()
    {
        return writableBytes() > 0;
    }

    @Override
    public int writableBytes()
    {
        return slice.length() - index;
    }

    @Override
    public void writeByte(int value)
    {
        slice = Slices.ensureSize(slice, index + 1);
        slice.setByte(index++, value);
    }

    @Override
    public void writeShort(int value)
    {
        slice = Slices.ensureSize(slice, index + 2);
        slice.setShort(index, value);
        index += 2;
    }

    @Override
    public void writeInt(int value)
    {
        slice = Slices.ensureSize(slice, index + 4);
        slice.setInt(index, value);
        index += 4;
    }

    @Override
    public void writeLong(long value)
    {
        slice = Slices.ensureSize(slice, index + 8);
        slice.setLong(index, value);
        index += 8;
    }

    @Override
    public void writeBytes(byte[] source)
    {
        writeBytes(source, 0, source.length);
    }

    @Override
    public void writeBytes(byte[] source, int sourceIndex, int length)
    {
        slice = Slices.ensureSize(slice, index + length);
        slice.setBytes(index, source, sourceIndex, length);
        index += length;
    }

    @Override
    public void writeBytes(Slice source)
    {
        writeBytes(source, 0, source.length());
    }

    @Override
    public void writeBytes(SliceInput source, int length)
    {
        if (length > source.available()) {
            throw new IndexOutOfBoundsException();
        }
        writeBytes(source.slice());
    }

    @Override
    public void writeBytes(Slice source, int sourceIndex, int length)
    {
        slice = Slices.ensureSize(slice, index + length);
        slice.setBytes(index, source, sourceIndex, length);
        index += length;
    }

    @Override
    public void writeBytes(ByteBuffer source)
    {
        int length = source.remaining();
        slice = Slices.ensureSize(slice, index + length);
        slice.setBytes(index, source);
        index += length;
    }

    @Override
    public int writeBytes(InputStream in, int length)
            throws IOException
    {
        slice = Slices.ensureSize(slice, index + length);
        int writtenBytes = slice.setBytes(index, in, length);
        if (writtenBytes > 0) {
            index += writtenBytes;
        }
        return writtenBytes;
    }

    @Override
    public int writeBytes(ScatteringByteChannel in, int length)
            throws IOException
    {
        slice = Slices.ensureSize(slice, index + length);
        int writtenBytes = slice.setBytes(index, in, length);
        if (writtenBytes > 0) {
            index += writtenBytes;
        }
        return writtenBytes;
    }

    @Override
    public int writeBytes(FileChannel in, int position, int length)
            throws IOException
    {
        slice = Slices.ensureSize(slice, index + length);
        int writtenBytes = slice.setBytes(index, in, position, length);
        if (writtenBytes > 0) {
            index += writtenBytes;
        }
        return writtenBytes;
    }

    @Override
    public void writeZero(int length)
    {
        if (length == 0) {
            return;
        }
        if (length < 0) {
            throw new IllegalArgumentException(
                    "length must be 0 or greater than 0.");
        }
        slice = Slices.ensureSize(slice, index + length);
        int nLong = length >>> 3;
        int nBytes = length & 7;
        for (int i = nLong; i > 0; i--) {
            writeLong(0);
        }
        if (nBytes == 4) {
            writeInt(0);
        }
        else if (nBytes < 4) {
            for (int i = nBytes; i > 0; i--) {
                writeByte((byte) 0);
            }
        }
        else {
            writeInt(0);
            for (int i = nBytes - 4; i > 0; i--) {
                writeByte((byte) 0);
            }
        }
    }

    @Override
    public Slice slice()
    {
        return slice.slice(0, index);
    }

    @Override
    public ByteBuffer toByteBuffer()
    {
        return slice.toByteBuffer(0, index);
    }

    @Override
    public String toString()
    {
        return getClass().getSimpleName() + '(' +
                "size=" + index + ", " +
                "capacity=" + slice.length() +
                ')';
    }

    @Override
    public String toString(Charset charset)
    {
        return slice.toString(0, index, charset);
    }
}
