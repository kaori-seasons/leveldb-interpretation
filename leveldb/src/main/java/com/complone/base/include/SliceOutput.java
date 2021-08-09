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

import java.io.DataOutput;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.ScatteringByteChannel;
import java.nio.charset.Charset;

public abstract class SliceOutput extends OutputStream
        implements DataOutput {
    /**
     * 设置流的位置为起始位置
     */
    public abstract void reset();

    /**
     * 返回当前buffer的index
     */
    public abstract int size();

    /**
     * 返回可写的byte数量，等于buffer的容量-当前index
     */
    public abstract int writableBytes();

    /**
     * 可写的byte数量大于0.返回true，否则返回false
     */
    public abstract boolean isWritable();

    @Override
    public final void writeBoolean(boolean value)
    {
        writeByte(value ? 1 : 0);
    }

    @Override
    public final void write(int value)
    {
        writeByte(value);
    }

    /**
     * 在指定的位置写入int值，int占4字节，共4 * 8 = 32位，但是一个byte只有8位，所以int的高24位会被忽略
     * 写完之后index+1
     * @throws IndexOutOfBoundsException 如果可写的byte数量 < 1
     */
    @Override
    public abstract void writeByte(int value);

    /**
     * 在指定的位置写入int值，int占4字节，共4 * 8 = 32位，但是一个short只有16位，所以int的高16位会被忽略
     * 写完之后index+2
     * @throws IndexOutOfBoundsException 如果可写的byte数量 < 2
     */
    @Override
    public abstract void writeShort(int value);

    /**
     * 在指定的位置写入int值，int占4字节
     * 写完之后index+4
     * @throws IndexOutOfBoundsException 如果可写的byte数量 < 4
     */
    @Override
    public abstract void writeInt(int value);

    /**
     * 在指定的位置写入long值，long占8字节
     * 写完之后index+8
     * @throws IndexOutOfBoundsException 如果可写的byte数量 < 8
     */
    @Override
    public abstract void writeLong(long value);

    /**
     * 将指定的source 缓存中的数据写入到Slice中，在目标Slice的index处开始写入，直到源Slice变为不可读，修改index位置。
     * 该方法与 {@link #writeBytes(Slice, int, int)}相同，除了该方法index增加的数值由源Slice决定，而
     * {@link #writeBytes(Slice, int, int)}由参数决定。
     *
     * @throws IndexOutOfBoundsException 如果源Slice的可读byte数量大于目标Slice的剩余byte数
     */
    public abstract void writeBytes(Slice source);

    /**
     * 将指定的source 缓存中的数据写入到Slice中，在目标Slice的index处开始写入，共写入length个byte。
     *
     * @param length 写入byte的数量
     * @throws IndexOutOfBoundsException 如果length大于目标Slice的剩余byte数 or length大于源Slice的剩余byte数
     */
    public abstract void writeBytes(SliceInput source, int length);

    /**
     * 将指定的source 缓存中的数据写入到Slice中，从源source的sourceIndex开始写入，共写入length个byte。
     *
     * @param sourceIndex 从源Slice的sourceIndex处开始写入
     * @param length 写入byte的数量
     * @throws IndexOutOfBoundsException 如果sourceIndex小于0 or {@code sourceIndex + length}大于source.data.length or
     * {@code length}大于目标Slice的可写长度
     */
    public abstract void writeBytes(Slice source, int sourceIndex, int length);

    @Override
    public final void write(byte[] source)
            throws IOException
    {
        writeBytes(source);
    }

    /**
     * 将source数组的数据写入该Slice的data，从该Slice的index开始写入，index+=source.length
     *
     * @throws IndexOutOfBoundsException 如果{@code index+source.length}大于目标Slice的可写长度
     */
    public abstract void writeBytes(byte[] source);

    @Override
    public final void write(byte[] source, int sourceIndex, int length)
    {
        writeBytes(source, sourceIndex, length);
    }

    /**
     * 将source数组的数据写入该Slice的data，从该Slice的index开始写入，共写入length个字节，目的Slice的index+=length
     * @param sourceIndex 从源Slice的sourceIndex开始写入
     * @param length 写入的byte数量为length
     * @throws IndexOutOfBoundsException 如果sourceIndex小于0 or {@code sourceIndex + length}大于source.length or
     * length 小于目的Slice的可写字节数。
     */
    public abstract void writeBytes(byte[] source, int sourceIndex, int length);

    /**
     * 将ByteBuffer的数据写入目标Slice，直到源ByteBuffer的position到了limit的位置。目标index+写入的byte数量。
     * remaining = limit - position，关于Buffer的一些问题，请参照本篇博客：
     * https://blog.csdn.net/u010659877/article/details/108864125
     *
     * @throws IndexOutOfBoundsException 如果{@code source.remaining()} 大于目标Slice的可写byte数量
     */
    public abstract void writeBytes(ByteBuffer source);

    /**
     * 将InputStream的数据写入目标Slice。目标index+=length。
     *
     * @param length 写入的byte数量
     * @return 实际写入的byte数量
     * @throws IndexOutOfBoundsException 如果 {@code length} 大于目的Slice的可写byte数量
     * @throws java.io.IOException 如果InputStream抛出I/O异常
     */
    public abstract int writeBytes(InputStream in, int length)
            throws IOException;

    /**
     * 将ScatteringByteChannel的数据写入到目标Slice中，最大写入byte数量为length
     *
     * @param length 最大可写数量
     * @return 实际写入的字节数
     * @throws IndexOutOfBoundsException 如果 {@code length} 大于目标Slice的可写字节数
     * @throws java.io.IOException 如果ScatteringByteChannel在读的过程中抛出了I/O异常
     */
    public abstract int writeBytes(ScatteringByteChannel in, int length)
            throws IOException;

    public abstract int writeBytes(FileChannel in, int position, int length)
            throws IOException;

    /**
     * 从该Slice的当前位置开始，填入length个0，index+=length
     *
     * @param length 写入的O的个数
     * @throws IndexOutOfBoundsException 如果length大于该Slice的剩余可写byte数
     */
    public abstract void writeZero(int length);

    /**
     * @ return Slice 返回一个Slice，返回的Slice的data是该Slice的data的浅拷贝，也就是说，修改返回的Slice的数据会造成
     * 该Slice的数据的修改，但是返回的Slice维护自己独立的index和其他标志。
     */
    public abstract Slice slice();

    /**
     * 将该Slice的数据转换为ByteBuffer格式，返回的ByteBuffer的数据既可能是Slice数据的浅拷贝，也可能是深拷贝，但是他们维护各自
     * 独立的index和标志。
     */
    public abstract ByteBuffer toByteBuffer();

    /**
     * 将该Buffer的数据以指定的字符集转换为String
     *
     * @throws java.nio.charset.UnsupportedCharsetException 如果jvm不支持该字符集
     */
    public abstract String toString(Charset charset);

    //
    // 不支持的操作
    //

    /**
     * 不支持的操作
     *
     * @throws UnsupportedOperationException always
     */
    @Override
    public void writeChar(int value)
    {
        throw new UnsupportedOperationException();
    }

    /**
     * 不支持的操作
     *
     * @throws UnsupportedOperationException always
     */
    @Override
    public void writeFloat(float v)
    {
        throw new UnsupportedOperationException();
    }

    /**
     * 不支持的操作
     *
     * @throws UnsupportedOperationException always
     */
    @Override
    public void writeDouble(double v)
    {
        throw new UnsupportedOperationException();
    }

    /**
     * 不支持的操作
     *
     * @throws UnsupportedOperationException always
     */
    @Override
    public void writeChars(String s)
    {
        throw new UnsupportedOperationException();
    }

    /**
     * 不支持的操作
     *
     * @throws UnsupportedOperationException always
     */
    @Override
    public void writeUTF(String s)
    {
        throw new UnsupportedOperationException();
    }

    /**
     * 不支持的操作
     *
     * @throws UnsupportedOperationException always
     */
    @Override
    public void writeBytes(String s)
    {
        throw new UnsupportedOperationException();
    }
}
