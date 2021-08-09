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
package com.complone.base.db;

import com.complone.base.include.Slice;
import com.complone.base.include.SliceInput;
import com.complone.base.include.SliceOutput;
import com.complone.base.utils.Coding;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.CharBuffer;
import java.nio.charset.*;
import java.util.IdentityHashMap;
import java.util.Map;

import static java.util.Objects.requireNonNull;


public class Slices {
    /**
     * 长度为0的缓存
     */
    public static final Slice EMPTY_SLICE = new Slice(0);

    public static Slice readLengthPrefixedBytes(SliceInput sliceInput)
    {
        int length = Coding.decodeInt(sliceInput);
        return sliceInput.readBytes(length);
    }

    public static void writeLengthPrefixedBytes(SliceOutput sliceOutput, Slice value)
    {
        Coding.encodeInt(value.length(), sliceOutput);
        sliceOutput.writeBytes(value);
    }

    private Slices()
    {
    }

    /**
     * 可确保容量至少等于指定的minimumCapacity，类似于StringBufferd的ensureCapacity()方法
     *
     * @param existingSlice 待确认capacity的Slice
     * @param minWritableBytes 要保证的最小容量
     * @return Slice
     */
    public static Slice ensureSize(Slice existingSlice, int minWritableBytes)
    {
        if (existingSlice == null) {
            existingSlice = EMPTY_SLICE;
        }
        // 如果可读字节数小于Slice的长度，直接返回该Slice
        if (minWritableBytes <= existingSlice.length()) {
            return existingSlice;
        }

        int newCapacity;
        if (existingSlice.length() == 0) {
            newCapacity = 1;
        }
        else {
            newCapacity = existingSlice.length();
        }
        int minNewCapacity = existingSlice.length() + minWritableBytes;
        while (newCapacity < minNewCapacity) {
            newCapacity <<= 1;
        }

        Slice newSlice = allocate(newCapacity);
        newSlice.setBytes(0, existingSlice, 0, existingSlice.length());
        return newSlice;
    }

    /**
     * 分配一个新的字节缓冲区。新缓冲区的位置将为零，其界限将为其容量
     * @param capacity 缓冲容量
     * @return 新建的缓冲区
     */
    public static Slice allocate(int capacity)
    {
        if (capacity == 0) {
            return EMPTY_SLICE;
        }
        return new Slice(capacity);
    }

    /**
     * 将数据包装到缓冲区
     * @param array Slice的data指针指向传入的数组，是浅拷贝
     * @return Slice
     */
    public static Slice wrappedBuffer(byte[] array)
    {
        if (array.length == 0) {
            return EMPTY_SLICE;
        }
        return new Slice(array);
    }

    /**
     * 将ByteBuffer的数据写入到Slice中，深拷贝
     * clear()方法不会真正的删除掉buffer中的数据，只是把position移动到最前面，同时把limit调整为capacity。源码：
     * public final Buffer clear() {
     *     position = 0;
     *     limit = capacity;
     *     mark = -1;
     *     return this;
     * }
     * @param source 源缓存
     * @param sourceOffset 源缓存偏移量
     * @param length 读取缓存的长度
     * @return 拷贝一份缓存
     */
    public static Slice copiedBuffer(ByteBuffer source, int sourceOffset, int length)
    {
        requireNonNull(source, "source is null");
        int newPosition = source.position() + sourceOffset;
        return copiedBuffer((ByteBuffer) source.duplicate().order(ByteOrder.LITTLE_ENDIAN).clear().limit(newPosition + length).position(newPosition));
    }

    /**
     * 拷贝缓存，深拷贝
     * @param source 源缓存
     * @return 拷贝后的缓存
     */
    public static Slice copiedBuffer(ByteBuffer source)
    {
        requireNonNull(source, "source is null");
        Slice copy = allocate(source.limit() - source.position());
        copy.setBytes(0, source.duplicate().order(ByteOrder.LITTLE_ENDIAN));
        return copy;
    }

    /**
     * 将字符串按照指定的字符集存储到缓存中
     * @param string 源字符串
     * @param charset 字符集
     * @return 包装成的缓存
     */
    public static Slice copiedBuffer(String string, Charset charset)
    {
        requireNonNull(string, "string is null");
        requireNonNull(charset, "charset is null");

        return wrappedBuffer(string.getBytes(charset));
    }

    private static final ThreadLocal<Map<Charset, CharsetDecoder>> decoders =
            new ThreadLocal<Map<Charset, CharsetDecoder>>()
            {
                @Override
                protected Map<Charset, CharsetDecoder> initialValue()
                {
                    return new IdentityHashMap<>();
                }
            };

    private static final ThreadLocal<Map<Charset, CharsetEncoder>> encoders =
            new ThreadLocal<Map<Charset, CharsetEncoder>>()
            {
                @Override
                protected Map<Charset, CharsetEncoder> initialValue()
                {
                    return new IdentityHashMap<>();
                }
            };

    public static String decodeString(ByteBuffer src, Charset charset)
    {
        CharsetDecoder decoder = getDecoder(charset);
        /** maxCharsPerByte()方法是java.nio.charset.CharsetDecoder类的内置方法，该方法返回为每个输入字节生成的最大字符数。
         * 该值可用于计算给定输入序列所需的输出缓冲区的最坏情况大小。
         *
         * 缓存区的实现原理：https://blog.csdn.net/u010659877/article/details/108864125
         * allocate是内部创建数组，wrap是通过传入外部数组创建
         * decode实现原理：https://blog.csdn.net/u010659877/article/details/109192298
         * */
        CharBuffer dst = CharBuffer.allocate(
                (int) ((double) src.remaining() * decoder.maxCharsPerByte()));
        try {
            CoderResult cr = decoder.decode(src, dst, true);
            if (!cr.isUnderflow()) {
                cr.throwException();
            }
            cr = decoder.flush(dst);
            if (!cr.isUnderflow()) {
                cr.throwException();
            }
        }
        catch (CharacterCodingException x) {
            throw new IllegalStateException(x);
        }
        return dst.flip().toString();
    }


    /**
     * 根据指定的<tt>charset</tt>返回一个缓存的threadlocal {@link CharsetDecoder}
     */
    private static CharsetDecoder getDecoder(Charset charset)
    {
        if (charset == null) {
            throw new NullPointerException("charset");
        }

        Map<Charset, CharsetDecoder> map = decoders.get();
        CharsetDecoder d = map.get(charset);
        if (d != null) {
            // 将CharsetDecoder的state设为0
            d.reset();
            // 对错误输入的操作使用CodingErrorAction.REPLACE
            d.onMalformedInput(CodingErrorAction.REPLACE);
            // 对不可映射的字符错误的操作使用CodingErrorAction.REPLACE
            d.onUnmappableCharacter(CodingErrorAction.REPLACE);
            return d;
        }

        d = charset.newDecoder();
        d.onMalformedInput(CodingErrorAction.REPLACE);
        d.onUnmappableCharacter(CodingErrorAction.REPLACE);
        map.put(charset, d);
        return d;
    }

    public static ByteBuffer encodeString(CharBuffer src, Charset charset)
    {
        CharsetEncoder encoder = getEncoder(charset);
        ByteBuffer dst = ByteBuffer.allocate(
                (int) ((double) src.remaining() * encoder.maxBytesPerChar()));
        try {
            CoderResult cr = encoder.encode(src, dst, true);
            if (!cr.isUnderflow()) {
                cr.throwException();
            }
            cr = encoder.flush(dst);
            if (!cr.isUnderflow()) {
                cr.throwException();
            }
        }
        catch (CharacterCodingException x) {
            throw new IllegalStateException(x);
        }
        dst.flip();
        return dst;
    }

    /**
     * 根据指定的<tt>charset</tt>返回一个缓存的threadlocal {@link CharsetEncoder}
     */
    private static CharsetEncoder getEncoder(Charset charset)
    {
        if (charset == null) {
            throw new NullPointerException("charset");
        }

        Map<Charset, CharsetEncoder> map = encoders.get();
        CharsetEncoder e = map.get(charset);
        if (e != null) {
            e.reset();
            e.onMalformedInput(CodingErrorAction.REPLACE);
            e.onUnmappableCharacter(CodingErrorAction.REPLACE);
            return e;
        }

        e = charset.newEncoder();
        e.onMalformedInput(CodingErrorAction.REPLACE);
        e.onUnmappableCharacter(CodingErrorAction.REPLACE);
        map.put(charset, e);
        return e;
    }


}
