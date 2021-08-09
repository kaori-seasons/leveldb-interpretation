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

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.ScatteringByteChannel;
import java.nio.charset.Charset;

public class BasicSliceOutput extends SliceOutput {
    private final Slice slice;
    private int index;
    protected BasicSliceOutput(Slice slice)
    {
        /**
         * 这里是浅拷贝
         */
        this.slice = slice;
    }

    /**
     * 这里的size其实是index
     */
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

    /**
     * 返回可写的byte数量，即slice的长度减去已写byte的数量
     * @return
     */
    @Override
    public int writableBytes()
    {
        return slice.length() - index;
    }

    @Override
    public void writeByte(int value)
    {
        slice.setByte(index++, value);
    }

    @Override
    public void writeShort(int value)
    {
        slice.setShort(index, value);
        index += 2;
    }

    @Override
    public void writeInt(int value)
    {
        slice.setInt(index, value);
        index += 4;
    }

    @Override
    public void writeLong(long value)
    {
        slice.setLong(index, value);
        index += 8;
    }

    @Override
    public void writeBytes(byte[] source, int sourceIndex, int length)
    {
        slice.setBytes(index, source, sourceIndex, length);
        index += length;
    }

    @Override
    public void writeBytes(byte[] source)
    {
        writeBytes(source, 0, source.length);
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
        writeBytes(source.readBytes(length));
    }

    @Override
    public void writeBytes(Slice source, int sourceIndex, int length)
    {
        slice.setBytes(index, source, sourceIndex, length);
        index += length;
    }

    @Override
    public void writeBytes(ByteBuffer source)
    {
        int length = source.remaining();
        slice.setBytes(index, source);
        index += length;
    }

    @Override
    public int writeBytes(InputStream in, int length)
            throws IOException
    {
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
        int writtenBytes = slice.setBytes(index, in, position, length);
        if (writtenBytes > 0) {
            index += writtenBytes;
        }
        return writtenBytes;
    }

    /**
     * 由于底层的数据结构式byte[]，所以我们这里是要写入length个byte，每个byte的值是0。
     * 为了尽可能的减少循环次数，提高效率，将length除以8，也就是无符号右移3位，>>> 3，这样就将写入length个byte转换为
     * 写length >>> 3个long了，由于右移了3位，还有3位漏网之鱼要单独处理。
     * 剩余的3位：
     * length & 7的结果是取length的后三位，对这3位单独处理。
     *
     * @param length 写入的O的个数
     */
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
        /**
         * 给length位byte赋值为0，我们可以拆分一个函数，就是把long写入byte[]中，writeLong每被调用一次，就解决了8个长度的赋值问题，
         * 因此可以把赋值length位的需求改为赋值length / 8 位的需求，为了表达的更直观，我们可以用无符号右移3位代替除以8。
         * 因为刚刚右移了3位，所以最后3位表示的个数还没有写到数组中，length & 7的结果可以得到length的最后三位的数值，
         * 对该值进行单独处理。
         */

        // 无符号右移3位相当于除以8
        int nLong = length >>> 3;
        // 7的二进制是0111，与7做与运算相当于只取最后三位
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

    public String toString(Charset charset)
    {
        return slice.toString(0, index, charset);
    }
}
