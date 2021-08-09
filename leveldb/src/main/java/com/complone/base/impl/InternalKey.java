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

import com.complone.base.utils.DataUnit;
import com.google.common.base.Preconditions;
import com.complone.base.db.Slices;
import com.complone.base.include.Slice;
import com.complone.base.include.SliceOutput;

import static com.google.common.base.Preconditions.checkArgument;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;

/**
 * InternalKey是由User key + SequenceNumber + ValueType组合而成的
 * InternalKey的格式为：
 * | User key (string) | sequence number (7 bytes) | value type (1 byte) |
 * sequence number大小是7 bytes，sequence number是所有基于op log系统的关键数据，
 * 它唯一指定了不同操作的时间顺序。
 * 把user key放到前面的原因是，这样对同一个user key的操作
 * 就可以按照sequence number顺序连续存放了，不同的user key是互不相干的，
 * 因此把它们的操作放在一起也没有什么意义。
 */
public class InternalKey
{
    private final Slice userKey;
    private final long sequenceNumber;
    private final ValueType valueType;

    public InternalKey(Slice userKey, long sequenceNumber, ValueType valueType)
    {
        requireNonNull(userKey, "userKey is null");
        checkArgument(sequenceNumber >= 0, "sequenceNumber is negative");
        requireNonNull(valueType, "valueType is null");

        this.userKey = userKey;
        this.sequenceNumber = sequenceNumber;
        this.valueType = valueType;
    }

    public InternalKey(Slice data)
    {
        requireNonNull(data, "data is null");
        Preconditions.checkArgument(data.length() >= DataUnit.LONG_UNIT, "data must be at least %s bytes", DataUnit.LONG_UNIT);
        this.userKey = getUserKey(data);
        // data 的最后8 byte是SequenceNumber和valueType的组合
        long packedSequenceAndType = data.getLong(data.length() - DataUnit.LONG_UNIT);
        this.sequenceNumber = SequenceNumber.unpackSequenceNumber(packedSequenceAndType);
        this.valueType = SequenceNumber.unpackValueType(packedSequenceAndType);
    }

    public InternalKey(byte[] data)
    {
        this(Slices.wrappedBuffer(data));
    }

    public Slice getUserKey()
    {
        return userKey;
    }

    public long getSequenceNumber()
    {
        return sequenceNumber;
    }

    public ValueType getValueType()
    {
        return valueType;
    }

    public Slice encode()
    {
        Slice slice = Slices.allocate(userKey.length() + DataUnit.LONG_UNIT);
        SliceOutput sliceOutput = slice.output();
        sliceOutput.writeBytes(userKey);
        sliceOutput.writeLong(SequenceNumber.packSequenceAndValueType(sequenceNumber, valueType));
        return slice;
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

        InternalKey that = (InternalKey) o;

        if (sequenceNumber != that.sequenceNumber) {
            return false;
        }
        if (userKey != null ? !userKey.equals(that.userKey) : that.userKey != null) {
            return false;
        }
        if (valueType != that.valueType) {
            return false;
        }

        return true;
    }

    private int hash;

    @Override
    public int hashCode()
    {
        if (hash == 0) {
            int result = userKey != null ? userKey.hashCode() : 0;
            result = 31 * result + (int) (sequenceNumber ^ (sequenceNumber >>> 32));
            result = 31 * result + (valueType != null ? valueType.hashCode() : 0);
            if (result == 0) {
                result = 1;
            }
            hash = result;
        }
        return hash;
    }

    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder();
        sb.append("InternalKey");
        sb.append("{key=").append(getUserKey().toString(UTF_8));      // todo don't print the real value
        sb.append(", sequenceNumber=").append(getSequenceNumber());
        sb.append(", valueType=").append(getValueType());
        sb.append('}');
        return sb.toString();
    }

    private static Slice getUserKey(Slice data)
    {
        return data.slice(0, data.length() - DataUnit.LONG_UNIT);
    }
}

