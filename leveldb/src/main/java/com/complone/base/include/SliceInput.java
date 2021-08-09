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

import java.io.DataInput;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.GatheringByteChannel;
import java.nio.charset.Charset;

public final class SliceInput
        extends InputStream
        implements DataInput
{
    private final Slice slice;
    private int position;

    public SliceInput(Slice slice)
    {
        this.slice = slice;
    }

    /**
     * 返回缓存数据的索引 {@code position}
     */
    public int position()
    {
        return position;
    }

    /**
     * 设置缓存数据的索引 {@code position}
     *
     * @throws IndexOutOfBoundsException
     */
    public void setPosition(int position)
    {
        if (position < 0 || position > slice.length()) {
            throw new IndexOutOfBoundsException();
        }
        this.position = position;
    }

    /**
     * 如果可读的字节数大于0，证明该SliceInput可读，返回 {@code true}
     */
    public boolean isReadable()
    {
        return available() > 0;
    }

    /**
     * 返回可读的byte数量，为 {@code (this.slice.length() - this.position)}.
     */
    @Override
    public int available()
    {
        return slice.length() - position;
    }

    /***
     * 将该位byte转为boolean返回
     * @return Boolean
     * @throws IOException
     */
    @Override
    public boolean readBoolean()
            throws IOException
    {
        return readByte() != 0;
    }

    /**
     * 默认读取字节，转为int返回
     * @return int
     */
    @Override
    public int read()
    {
        return readByte();
    }

    /**
     * 都当前position的一个byte，position往后指一位，{@code 1}加一
     *
     * @throws IndexOutOfBoundsException
     */
    @Override
    public byte readByte()
    {
        if (position == slice.length()) {
            throw new IndexOutOfBoundsException();
        }
        return slice.getByte(position++);
    }

    /**
     * 将该位作为无符号字节返回，byte & 0xFF可将字节转为无符号字节，用int表示
     *
     * @throws IndexOutOfBoundsException
     */
    @Override
    public int readUnsignedByte()
    {
        return (short) (readByte() & 0xFF);
    }

    /**
     * 在当前索引 {@code position} 处读2字节作为short， {@code position} + 2
     *
     * @throws IndexOutOfBoundsException
     */
    @Override
    public short readShort()
    {
        short v = slice.getShort(position);
        position += 2;
        return v;
    }

    /**
     *
     * @return 返回无符号的short，返回类型为int
     * @throws IOException
     */
    @Override
    public int readUnsignedShort()
            throws IOException
    {
        return readShort() & 0xff;
    }

    /**
     * @return 32位的int，@{code position} + 4
     *
     * @throws IndexOutOfBoundsException
     */
    @Override
    public int readInt()
    {
        int v = slice.getInt(position);
        position += 4;
        return v;
    }

    /**
     * 返回32位的无符号int，无符号int只能用long来表示，否则位数不够，因为32位都要用老表示值
     *
     * @throws IndexOutOfBoundsException
     */
    public long readUnsignedInt()
    {
        return readInt() & 0xFFFFFFFFL;
    }

    /**
     * @return 64位的long，@{code position} + 8
     *
     * @throws IndexOutOfBoundsException if {@code this.available()} is less than {@code 8}
     */
    @Override
    public long readLong()
    {
        long v = slice.getLong(position);
        position += 8;
        return v;
    }

    /**
     * 返回@param length长度的数组
     * @return
     */
    public byte[] readByteArray(int length)
    {
        byte[] value = slice.copyBytes(position, length);
        position += length;
        return value;
    }

    /**
     * 将缓存的数据从{@code position}新建一个长度为{@code length}的副本，将{@code position}向后移length位
     *
     * @param length 新的Slice的长度
     * @return 新的Slice，偏移量就是原来的偏移量
     * @throws IndexOutOfBoundsException if {@code length} is greater than {@code this.available()}
     */
    public Slice readBytes(int length)
    {
        if (length == 0) {
            return Slices.EMPTY_SLICE;
        }
        Slice value = slice.slice(position, length);
        position += length;
        return value;
    }

    /**
     * 将缓存的数据从{@code position}新建一个长度为{@code length}的副本，将{@code position}向后移length位
     *
     * @param length 新的Slice的长度
     * @return 新的Slice，偏移量就是原来的偏移量
     * @throws IndexOutOfBoundsException
     */
    public Slice readSlice(int length)
    {
        Slice newSlice = slice.slice(position, length);
        position += length;
        return newSlice;
    }

    @Override
    public void readFully(byte[] destination)
    {
        readBytes(destination);
    }

    /**
     * 将缓存的数据拷贝到destination数组中，从0到数组末尾读满
     * @throws IndexOutOfBoundsException
     */
    public void readBytes(byte[] destination)
    {
        readBytes(destination, 0, destination.length);
    }

    /**
     * 将缓存的数据拷贝到destination数组中，拷贝的起始位置是position，目标数组的起始位置是offset，长度是length
     * @throws IndexOutOfBoundsException
     */
    @Override
    public void readFully(byte[] destination, int offset, int length)
    {
        readBytes(destination, offset, length);
    }

    /**
     * 将缓存的数据拷贝到destination数组中，拷贝的起始位置是position，目标数组的起始位置是destinationIndex，长度是length，
     * position指针+length
     * @throws IndexOutOfBoundsException
     */
    public void readBytes(byte[] destination, int destinationIndex, int length)
    {
        slice.getBytes(position, destination, destinationIndex, length);
        position += length;
    }

    /**
     * 将缓存中的数据拷贝到目标Slice，即destination中，直到destination被写满，position增加destination.length()
     *
     * @throws IndexOutOfBoundsException
     */
    public void readBytes(Slice destination)
    {
        readBytes(destination, destination.length());
    }

    /**
     * 将缓存中的数据拷贝到目标Slice，即destination中，拷贝数据的长度为length，position增加length
     *
     * @throws IndexOutOfBoundsException
     */
    public void readBytes(Slice destination, int length)
    {
        if (length > destination.length()) {
            throw new IndexOutOfBoundsException();
        }
        readBytes(destination, 0, length);
    }

    /**
     * 将当前缓存中的数据拷贝到目标Slice中，Slice的offset是destinationIndex，拷贝字节长度为length，position =+ length
     *
     * @throws IndexOutOfBoundsException
     */
    public void readBytes(Slice destination, int destinationIndex, int length)
    {
        slice.getBytes(position, destination, destinationIndex, length);
        position += length;
    }

    /**
     * 将缓存中的数据写到目标ByteBuffer中，直到ByteBuffer被写满，position增加ByteBuffer的剩余长度，
     * position + ByteBuffer的剩余长度
     *
     * @throws IndexOutOfBoundsException
     */
    public void readBytes(ByteBuffer destination)
    {
        int length = destination.remaining();
        slice.getBytes(position, destination);
        position += length;
    }

    /**
     * 将缓存中的数据写到目标GatheringByteChannel中，写的长度为length，position增加length
     * @return 读到的字节数量
     * @throws IndexOutOfBoundsException
     * @throws java.io.IOException
     */
    public int readBytes(GatheringByteChannel out, int length)
            throws IOException
    {
        int readBytes = slice.getBytes(position, out, length);
        position += readBytes;
        return readBytes;
    }

    /**
     * 将缓存中的数据写到目标OutputStream中，写的长度为length，position增加length
     *
     * @throws IndexOutOfBoundsException
     * @throws java.io.IOException
     */
    public void readBytes(OutputStream out, int length)
            throws IOException
    {
        slice.getBytes(position, out, length);
        position += length;
    }

    /**
     * 跳过多少个字节，如果剩余可读字节数量小于要求跳过的字节数，则跳过Math.min(length, available());
     * @param length 跳过字节数量
     * @return 实际跳过的字节数
     */
    public int skipBytes(int length)
    {
        length = Math.min(length, available());
        position += length;
        return length;
    }

    /**
     * 返回该缓冲区剩余可读字节，该缓冲区和返回的缓冲区都引用同一个data对象，因此修改内容会互相影响，但是他们维护独立的index
     * @retyrn Slice
     */
    public Slice slice()
    {
        return slice.slice(position, available());
    }

    /**
     * 返回该缓冲区剩余可读字节，该缓冲区和返回的缓冲区都引用同一个data对象，因此修改内容会互相影响，但是他们维护独立的index
     * @retyrn ByteBuffer
     */
    public ByteBuffer toByteBuffer()
    {
        return slice.toByteBuffer(position, available());
    }

    /**
     * 返回该缓冲区剩余可读字节，该缓冲区和返回的缓冲区都引用同一个data对象，因此修改内容会互相影响，但是他们维护独立的index
     * @throws java.nio.charset.UnsupportedCharsetException 如果虚拟机不支持该字符集
     */
    public String toString(Charset charset)
    {
        return slice.toString(position, available(), charset);
    }

    @Override
    public String toString()
    {
        return getClass().getSimpleName() + '(' +
                "ridx=" + position + ", " +
                "cap=" + slice.length() +
                ')';
    }

    //
    // 不支持的操作类型
    //

    /**
     * 异常处理，不支持的操作类型
     *
     * @throws UnsupportedOperationException 总是抛出该异常
     */
    @Override
    public char readChar()
    {
        throw new UnsupportedOperationException();
    }

    /**
     * 异常处理，不支持的操作类型
     *
     * @throws UnsupportedOperationException 总是抛出该异常
     */
    @Override
    public float readFloat()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public double readDouble()
    {
        throw new UnsupportedOperationException();
    }

    /**
     * 异常处理，不支持的操作类型
     *
     * @throws UnsupportedOperationException 总是抛出该异常
     */
    @Override
    public String readLine()
    {
        throw new UnsupportedOperationException();
    }

    /**
     * 异常处理，不支持的操作类型
     *
     * @throws UnsupportedOperationException 总是抛出该异常
     */
    @Override
    public String readUTF()
    {
        throw new UnsupportedOperationException();
    }
}
