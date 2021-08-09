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
package com.complone.base.impl;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

/**
 * leveldb每添加/修改一次记录都会触发顺序号的+1。
 */
public final class SequenceNumber
{
    // SequenceNumber是Key的一部分，占 7 byte，ValueType占1byte，组合之后就是long
    // ((0x1L << 56) - 1) 之后就可以获得最大的7 byte
    public static final long MAX_SEQUENCE_NUMBER = ((0x1L << 56) - 1);

    private SequenceNumber()
    {
    }
    // 组合SequenceNumber和 ValueType
    public static long packSequenceAndValueType(long sequence, ValueType valueType)
    {
        checkArgument(sequence <= MAX_SEQUENCE_NUMBER, "Sequence number is greater than MAX_SEQUENCE_NUMBER");
        requireNonNull(valueType, "valueType is null");

        return (sequence << 8) | valueType.getPersistentId();
    }
    // 从long中拆出 ValueType
    public static ValueType unpackValueType(long num)
    {
        return ValueType.getValueTypeByPersistentId((byte) num);
    }

    // 从long中拆出 SequenceNumber
    public static long unpackSequenceNumber(long num)
    {
        return num >>> 8;
    }
}