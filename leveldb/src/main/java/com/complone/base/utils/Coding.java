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
package com.complone.base.utils;

import com.complone.base.include.SliceInput;
import com.complone.base.include.SliceOutput;

import java.nio.ByteBuffer;

/**
 * 变长存储，也就是VarInt。
 * 对于VarInt，每byte的有效存储是7bit的，用最高的8bit位来表示是否结束，如果是1就表示后面还有一个byte的数字，否则表示结束。
 */
public class Coding {
    // 因为都是静态函数，不允许创建该类的对象，因此把构造函数设为private
    private Coding(){}

    /**
     * 有效位是7位byte，把int按7位分割
     * @param value 待转码数字
     * @return 转码后的字符长度
     */
    public static int varintLength(int value){
        int size = 1;
        while ((value & (~0x7f)) != 0) {
            value >>>= 7;
            size++;
        }
        return size;
    }
    public static int varintLength(long value){
        int size = 1;
        while ((value & (~0x7f)) != 0) {
            value >>>= 7;
            size++;
        }
        return size;
    }
    public static void encodeInt(int value, SliceOutput sliceOutput){
        int highBitMask = 0x80;
        if (value < (1 << 7) && value >= 0) {
            sliceOutput.writeByte(value);
        }
        else if (value < (1 << 14) && value > 0) {
            sliceOutput.writeByte(value | highBitMask);
            sliceOutput.writeByte(value >>> 7);
        }
        else if (value < (1 << 21) && value > 0) {
            sliceOutput.writeByte(value | highBitMask);
            sliceOutput.writeByte((value >>> 7) | highBitMask);
            sliceOutput.writeByte(value >>> 14);
        }
        else if (value < (1 << 28) && value > 0) {
            sliceOutput.writeByte(value | highBitMask);
            sliceOutput.writeByte((value >>> 7) | highBitMask);
            sliceOutput.writeByte((value >>> 14) | highBitMask);
            sliceOutput.writeByte(value >>> 21);
        }
        else {
            sliceOutput.writeByte(value | highBitMask);
            sliceOutput.writeByte((value >>> 7) | highBitMask);
            sliceOutput.writeByte((value >>> 14) | highBitMask);
            sliceOutput.writeByte((value >>> 21) | highBitMask);
            sliceOutput.writeByte(value >>> 28);
        }
    }
    public static void encodeLong(long value, SliceOutput sliceOutput){
        while ((value & (~0x7f)) != 0) {
            sliceOutput.writeByte((int) ((value & 0x7f) | 0x80));
            value >>>= 7;
        }
        sliceOutput.writeByte((int) value);
    }
    public static int decodeInt(SliceInput sliceInput){
        int result = 0;
        for (int shift = 0; shift <= 28; shift += 7) {
            int b = sliceInput.readUnsignedByte();

            // 将后7位与result相加
            result |= ((b & 0x7f) << shift);

            // 如果没有设置高位，则证明这是数组中最后一个字节
            if ((b & 0x80) == 0) {
                return result;
            }
        }
        throw new NumberFormatException("last byte of variable length int has high bit set");
    }
    public static long decodeLong(SliceInput sliceInput){
        long result = 0;
        for (int shift = 0; shift <= 63; shift += 7) {
            long b = sliceInput.readUnsignedByte();

            // 将后7位与result相加
            result |= ((b & 0x7f) << shift);

            // 如果没有设置高位，则证明这是数组中最后一个字节
            if ((b & 0x80) == 0) {
                return result;
            }
        }
        throw new NumberFormatException("last byte of variable length int has high bit set");
    }
    public static int decodeInt(ByteBuffer sliceInput)
    {
        int result = 0;
        for (int shift = 0; shift <= 28; shift += 7) {
            int b = sliceInput.get();

            result |= ((b & 0x7f) << shift);

            if ((b & 0x80) == 0) {
                return result;
            }
        }
        throw new NumberFormatException("last byte of variable length int has high bit set");
    }

}
