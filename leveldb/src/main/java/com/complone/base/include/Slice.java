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

import com.complone.base.utils.DataUnit;
import com.complone.base.db.Slices;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.FileChannel;
import java.nio.channels.GatheringByteChannel;
import java.nio.channels.ScatteringByteChannel;
import java.nio.charset.Charset;
import java.util.Arrays;

import static com.google.common.base.Preconditions.checkPositionIndex;
import static com.google.common.base.Preconditions.checkPositionIndexes;
import static java.nio.ByteOrder.LITTLE_ENDIAN;
import static java.util.Objects.requireNonNull;


public final class Slice {
    private final byte[] data;
    private final int offset;
    private final int length;

    private int hash;
    public Slice(){
        this.data = new byte[0];
        this.offset = 0;
        this.length = 0;
    }
    public Slice(String s){
        requireNonNull(s, "String is null");
        this.data = s.getBytes();
        this.offset = 0;
        this.length = s.length();
    }

    public Slice(byte[] data)
    {
        requireNonNull(data, "array is null");
        this.data = data;
        this.offset = 0;
        this.length = data.length;
    }

    public Slice(int length)
    {
        this.data = new byte[length];
        this.offset = 0;
        this.length = length;
    }

    public Slice(byte[] data, int offset, int length)
    {
        requireNonNull(data, "array is null");
        this.data = data;
        this.offset = offset;
        this.length = length;
    }

    /**
     * 返回数据长度，以byte计算
     * */
    public int length()
    {
        return this.length;
    }

    /**
     * 返回数据的头指针
     * */
    public byte[] getData()
    {
        return this.data;
    }

    /**
     * 判断数据是否为空
     * */
    public boolean empty(){
        return this.length == 0;
    }
    /**
     * 返回数据的偏移量
     */
    public int getOffset()
    {
        return this.offset;
    }

    /**
     * 返回指定index的byte
     * @throws IndexOutOfBoundsException
     */
    public byte getByte(int index)
    {
        checkPositionIndexes(index, index + DataUnit.BYTE_UNIT, this.length);
        index += this.offset;
        return this.data[index];
    }

    /**
     *
     * Java中所有的byte类型都是signed类型。只能表达（-128，127），signed byte可以表达（0，255）。
     * 通过将byte声明为short或者int类型。然后与0xFF取&。即可将signed byte转为unsigned byte。
     * 0xff 表示为二进制就是 1111 1111。在signed byte类型中，代表-1；但在short或者int类型中则代表255.
     * 当把byte类型的-1赋值到short或者int类型时，虽然值仍然代表-1，但却由1111 1111变成1111 1111 1111 1111.
     * 再将其与0xff进行掩码：
     * -1: 11111111 1111111
     * 0xFF: 00000000 1111111
     * 255: 00000000 1111111
     * 所以这样，-1就转换成255.
     *
     * @throws IndexOutOfBoundsException
     */
    public short getUnsignedByte(int index)
    {
        return (short) (getByte(index) & 0xFF);
    }

    /**
     * 返回16位的short，@param index 索引位置
     *
     * 知识点：
     * Leveldb对于数字的存储是little-endian的，在把int32或者int64转换为char*的函数中，
     * 是按照先低位再高位的顺序存放的，也就是little-endian的。所以，这里先要把高位左移8位，
     * 再与低位做或运算。
     *
     * @throws IndexOutOfBoundsException
     */
    public short getShort(int index)
    {
        checkPositionIndexes(index, index + DataUnit.SHORT_UNIT, this.length);
        index += this.offset;
        return (short) (this.data[index] & 0xFF | this.data[index + 1] << 8);
    }

    /**
     * 返回32位的int，@param index 索引位置
     *
     * @throws IndexOutOfBoundsException
     */
    public int getInt(int index)
    {
        checkPositionIndexes(index, index + DataUnit.INT_UNIT, this.length);
        index += offset;
        return (this.data[index] & 0xff) |
                (this.data[index + 1] & 0xff) << 8 |
                (this.data[index + 2] & 0xff) << 16 |
                (this.data[index + 3] & 0xff) << 24;
    }

    /**
     * 返回64位的long，@param index 索引位置
     *
     * @throws IndexOutOfBoundsException
     */
    public long getLong(int index)
    {
        checkPositionIndexes(index, index + DataUnit.LONG_UNIT, this.length);
        index += offset;
        return ((long) this.data[index] & 0xff) |
                ((long) this.data[index + 1] & 0xff) << 8 |
                ((long) this.data[index + 2] & 0xff) << 16 |
                ((long) this.data[index + 3] & 0xff) << 24 |
                ((long) this.data[index + 4] & 0xff) << 32 |
                ((long) this.data[index + 5] & 0xff) << 40 |
                ((long) this.data[index + 6] & 0xff) << 48 |
                ((long) this.data[index + 7] & 0xff) << 56;
    }

    /**
     * 把当前数据复制到目标Slice中
     */
    public void getBytes(int index, Slice dst, int dstIndex, int length)
    {
        getBytes(index, dst.data, dstIndex, length);
    }

    /**
     * 把当前数据复制到目标Slice中
     */
    public void getBytes(int index, byte[] destination, int destinationIndex, int length)
    {
        checkPositionIndexes(index, index + length, this.length);
        checkPositionIndexes(destinationIndex, destinationIndex + length, destination.length);
        index += offset;
        System.arraycopy(data, index, destination, destinationIndex, length);
    }

    public byte[] getBytes()
    {
        return getBytes(0, length);
    }

    /**
     * 返回byte[]类型的数据
     * */
    public byte[] getBytes(int index, int length)
    {
        index += offset;
        if (index == 0) {
            return Arrays.copyOf(data, length);
        }
        else {
            byte[] value = new byte[length];
            System.arraycopy(data, index, value, 0, length);
            return value;
        }
    }

    /**
     * 将数据写入ByteBuffer，这里因为ByteBuffer是对象，都是通过引用来实现的，因此并不需要返回对象本身
     */
    public void getBytes(int index, ByteBuffer destination)
    {
        checkPositionIndex(index, this.length);
        index += offset;
        destination.put(data, index, Math.min(length, destination.remaining()));
    }

    /**
     * 将数据写入OutputStream，这里因为OutputStream是对象，都是通过引用来实现的，因此并不需要返回对象本身
     */
    public void getBytes(int index, OutputStream out, int length)
            throws IOException
    {
        checkPositionIndexes(index, index + length, this.length);
        index += offset;
        out.write(data, index, length);
    }

    /**
     * https://blog.csdn.net/u010659877/article/details/108864125
     * 在我的这篇博客中，介绍了NIO的原理，其中有个概念叫channel，是双向的通道，将channel注册到Selector上，就可以基于NIO进行
     * IO操作，这里GatheringByteChannel是一个接口，实现类有DatagramChannel, FileChannel, Pipe.SinkChannel,
     * SocketChannel，是一个用于从多个缓冲区写入的通道的接口，有两个方法：
     * 具体原理参考我的博客：https://blog.csdn.net/u010659877/article/details/109012528
     * 1. public long write(ByteBuffer[] srcs, int offset, int length) throws IOException;
     * 2. public long write(ByteBuffer[] srcs) throws IOException;
     * @return 写入的字节数 如果是-1，代表通道关闭
     * 将数据写入GatheringByteChannel，这里因为GatheringByteChannel是对象，都是通过引用来实现的，因此并不需要返回对象本身
     */
    public int getBytes(int index, GatheringByteChannel out, int length)
            throws IOException
    {
        checkPositionIndexes(index, index + length, this.length);
        index += offset;
        return out.write(ByteBuffer.wrap(data, index, length));
    }

    /**
     * 将16位的short类型的int写入Slice中
     */
    public void setShort(int index, int value)
    {
        checkPositionIndexes(index, index + DataUnit.SHORT_UNIT, this.length);
        index += offset;
        data[index] = (byte) (value);
        data[index + 1] = (byte) (value >>> 8);
    }

    /**
     * 将int写入Slice中
     */
    public void setInt(int index, int value)
    {
        checkPositionIndexes(index, index + DataUnit.INT_UNIT, this.length);
        index += offset;
        data[index] = (byte) (value);
        data[index + 1] = (byte) (value >>> 8);
        data[index + 2] = (byte) (value >>> 16);
        data[index + 3] = (byte) (value >>> 24);
    }

    /**
     * 将long写入Slice中
     */
    public void setLong(int index, long value)
    {
        checkPositionIndexes(index, index + DataUnit.LONG_UNIT, this.length);
        index += offset;
        data[index] = (byte) (value);
        data[index + 1] = (byte) (value >>> 8);
        data[index + 2] = (byte) (value >>> 16);
        data[index + 3] = (byte) (value >>> 24);
        data[index + 4] = (byte) (value >>> 32);
        data[index + 5] = (byte) (value >>> 40);
        data[index + 6] = (byte) (value >>> 48);
        data[index + 7] = (byte) (value >>> 56);
    }

    /**
     * 将int中指定的某字节写入Slice
     */
    public void setByte(int index, int value)
    {
        checkPositionIndexes(index, index + DataUnit.BYTE_UNIT, this.length);
        index += offset;
        data[index] = (byte) value;
    }

    /**
     * 复制 Slice
     */
    public void setBytes(int index, Slice src, int srcIndex, int length)
    {
        setBytes(index, src.data, src.offset + srcIndex, length);
    }

    /**
     * 复制 Slice，深拷贝
     * 因为java中的对象都是通过引用进行操作的，数组也是一种引用，因此不用返回对象，copy操作的返回值是void就行
     */
    public void setBytes(int index, byte[] source, int sourceIndex, int length)
    {
        checkPositionIndexes(index, index + length, this.length);
        checkPositionIndexes(sourceIndex, sourceIndex + length, source.length);
        index += offset;
        System.arraycopy(source, sourceIndex, data, index, length);
    }

    /**
     *ByteBuffer.remaining() 返回limit - position;返回limit和position之间相对位置差
     * https://blog.csdn.net/u010659877/article/details/108864125这篇文章有什么是limit position
     *
     * 从position位置开始相对读，读length个byte，并写入dst下标从offset到offset+length的区域
     * get(byte[] dst, int offset, int length)
     * for (int i = off; i < off + len; i++)
     *     dst[i] = src.get();
     */
    public void setBytes(int index, ByteBuffer source)
    {
        checkPositionIndexes(index, index + source.remaining(), this.length);
        index += offset;
        source.get(data, index, source.remaining());
    }

    /**
     * 将InputStream的数据写入到Slice的data中
     * @return 写入的字节数
     * InputStream.read(byte[] b,int off,int len)
     * 将输入流中最多 len 个数据字节读入字节数组。尝试读取多达 len 字节，但可能读取较少数量。以整数形式返回实际读取的字节数。
     * 在输入数据可用、检测到流的末尾或者抛出异常前，此方法一直阻塞。
     * 类 InputStream 的 read(b, off, len) 方法只重复调用方法 read()。读取输入流的下一个字节，返回一个0-255之间的int类型整数。
     * 如果第一个这样的调用导致 IOException，则从对 read(b, off, len) 方法的调用中返回该异常。
     * 参数：
     *      b - 读入数据的缓冲区。
     *      off - 在其处写入数据的数组 b 的初始偏移量。
     *      len - 要读取的最大字节数。
     * @return 写入缓冲区的总字节数，如果由于已到达流末尾而不再有数据，则返回 -1。
     *
     */
    public int setBytes(int index, InputStream in, int length)
            throws IOException
    {
        checkPositionIndexes(index, index + length, this.length);
        index += offset;
        int readBytes = 0;
        do {
            int localReadBytes = in.read(data, index, length);
            if (localReadBytes < 0) {
                if (readBytes == 0) {
                    // channel关闭
                    return -1;
                }
                else {
                    break;
                }
            }
            readBytes += localReadBytes;
            index += localReadBytes;
            length -= localReadBytes;
        } while (length > 0);

        return readBytes;
    }

    /**
     * 将 ScatteringByteChannel的数据写入到Slice中，具体的read方法在下方链接处
     * https://blog.csdn.net/u010659877/article/details/108983748
     * ByteBuffer.wrap是将byte[]的一部分包装成ByteBuffer，
     * @return 写入缓冲区的总字节数，如果由于已到达流末尾而不再有数据，则返回 -1。
     */
    public int setBytes(int index, ScatteringByteChannel in, int length)
            throws IOException
    {
        checkPositionIndexes(index, index + length, this.length);
        index += offset;
        ByteBuffer buf = ByteBuffer.wrap(data, index, length);
        int readBytes = 0;

        do {
            int localReadBytes;
            try {
                localReadBytes = in.read(buf);
            }
            catch (ClosedChannelException e) {
                localReadBytes = -1;
            }
            if (localReadBytes < 0) {
                if (readBytes == 0) {
                    return -1;
                }
                else {
                    break;
                }
            }
            else if (localReadBytes == 0) {
                break;
            }
            readBytes += localReadBytes;
        } while (readBytes < length);

        return readBytes;
    }

    /**
     * FileChannel是ScatteringByteChannel的具体实现
     */
    public int setBytes(int index, FileChannel in, int position, int length)
            throws IOException
    {
        checkPositionIndexes(index, index + length, this.length);
        index += offset;
        ByteBuffer buf = ByteBuffer.wrap(data, index, length);
        int readBytes = 0;


        /**
         * ByteBuffer内存模型
         * pos + rem + limit
         */
        do {
            int localReadBytes;
            try {
                localReadBytes = in.read(buf, position + readBytes);
            }
            catch (ClosedChannelException e) {
                localReadBytes = -1;
            }
            if (localReadBytes < 0) {
                if (readBytes == 0) {
                    return -1;
                }
                else {
                    break;
                }
            }
            else if (localReadBytes == 0) {
                break;
            }
            readBytes += localReadBytes;
        } while (readBytes < length);

        return readBytes;
    }

    public Slice copySlice()
    {
        return copySlice(0, length);
    }

    /**
     * 深拷贝Slice的部分数据，返回Slice
     */
    public Slice copySlice(int index, int length)
    {
        checkPositionIndexes(index, index + length, this.length);

        index += offset;
        byte[] copiedArray = new byte[length];
        System.arraycopy(data, index, copiedArray, 0, length);
        return new Slice(copiedArray);
    }
    /**
     * 返回byte数组
     * */
    public byte[] copyBytes()
    {
        return copyBytes(0, length);
    }

    public byte[] copyBytes(int index, int length)
    {
        checkPositionIndexes(index, index + length, this.length);
        index += offset;
        if (index == 0) {
            return Arrays.copyOf(data, length);
        }
        else {
            byte[] value = new byte[length];
            System.arraycopy(data, index, value, 0, length);
            return value;
        }
    }

    /**
     * 返回Slice
     * */
    public Slice slice()
    {
        return slice(0, length);
    }

    /**
     * 返回部分Slice
     */
    public Slice slice(int index, int length)
    {
        if (index == 0 && length == this.length) {
            return this;
        }

        checkPositionIndexes(index, index + length, this.length);
        if (index >= 0 && length == 0) {
            return Slices.EMPTY_SLICE;
        }
        return new Slice(data, offset + index, length);
    }

    /**
     * 构造输入流
     */
    public SliceInput input()
    {
        return new SliceInput(this);
    }

    /**
     * 创建该Slice的输出流
     */
    public SliceOutput output()
    {
        return new BasicSliceOutput(this);
    }

    /**
     * 将Slice中的数据转换为ByteBuffer
     */
    public ByteBuffer toByteBuffer()
    {
        return toByteBuffer(0, length);
    }

    /**
     * 将Slice的部分数据转为NIO buffer，返回的buffer与Slice共享同一个对象，其中一个改变了，另一个也会改变。
     */
    public ByteBuffer toByteBuffer(int index, int length)
    {
        checkPositionIndexes(index, index + length, this.length);
        index += offset;
        return ByteBuffer.wrap(data, index, length).order(LITTLE_ENDIAN);
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

        Slice slice = (Slice) o;

        // 长度比较
        if (this.length != slice.length) {
            return false;
        }

        //偏移量和data比较
        if (offset == slice.offset && this.data == slice.data) {
            return true;
        }
        // data逐位比较
        for (int i = 0; i < this.length; i++) {
            if (this.data[this.offset + i] != slice.data[slice.offset + i]) {
                return false;
            }
        }
        return true;
    }

    @Override
    public int hashCode()
    {
        if (this.hash != 0) {
            return this.hash;
        }

        int result = this.length;
        /**
         * 之所以使用 31， 是因为他是一个奇素数。如果乘数是偶数，并且乘法溢出的话，信息就会丢失，因为与2相乘等价于移位运算（低位补0）。
         * 使用素数的好处并不很明显，但是习惯上使用素数来计算散列结果。 31 有个很好的性能，即用移位和减法来代替乘法，
         * 可以得到更好的性能： 31 * i == (i << 5） - i， 现代的 VM 可以自动完成这种优化。这个公式可以很简单的推导出来。
         * */
        for (int i = this.offset; i < this.offset + this.length; i++) {
            result = 31 * result + this.data[i];
        }
        if (result == 0) {
            result = 1;
        }
        this.hash = result;
        return this.hash;
    }

    /**
     * 先比较是不是同一个对象，对象的数据、长度、偏移量，都一致的时候返回0
     * 上述不一致的情况下，选择较小的长度，对data进行逐位比较，优先返回第一个不相同的字符差
     * 如果都相同，返回长度差
     * 这里有个小知识，byte转int的方法：
     * byte & 0xFF可以得到int
     */
    public int compareTo(Slice that)
    {
        if (this == that) {
            return 0;
        }
        if (this.data == that.data && length == that.length && offset == that.offset) {
            return 0;
        }

        int minLength = Math.min(this.length, that.length);
        for (int i = 0; i < minLength; i++) {
            int thisByte = 0xFF & this.data[this.offset + i];
            int thatByte = 0xFF & that.data[that.offset + i];
            if (thisByte != thatByte) {
                return (thisByte) - (thatByte);
            }
        }
        return this.length - that.length;
    }

    /**
     * 将数据以指定编码解码为String
     */
    public String toString(Charset charset)
    {
        return toString(0, length, charset);
    }

    /**
     * 将数据的一部分解码为String
     */
    public String toString(int index, int length, Charset charset)
    {
        if (length == 0) {
            return "";
        }

        return Slices.decodeString(toByteBuffer(index, length), charset);
    }
    /**
     * 返回长度信息
     */
    public String toString()
    {
        return getClass().getSimpleName() + '(' +
                "length=" + length() +
                ')';
    }

}
