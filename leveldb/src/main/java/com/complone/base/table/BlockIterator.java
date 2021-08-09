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

import com.complone.base.impl.SeekingIterator;
import com.complone.base.include.Slice;
import com.complone.base.include.SliceInput;
import com.complone.base.utils.Coding;
import com.complone.base.utils.DataUnit;
import com.google.common.base.Preconditions;
import com.complone.base.db.Slices;
import com.complone.base.include.SliceOutput;

import java.util.Comparator;
import java.util.NoSuchElementException;

import static com.google.common.base.Preconditions.*;
import static java.util.Objects.requireNonNull;

public class BlockIterator
        implements SeekingIterator<Slice, Slice>
{
    private final SliceInput data;
    private final Slice restartPositions;
    private final int restartCount;
    private final Comparator<Slice> comparator;

    private BlockEntry nextEntry;

    public BlockIterator(Slice data, Slice restartPositions, Comparator<Slice> comparator)
    {
        requireNonNull(data, "data is null");
        requireNonNull(restartPositions, "restartPositions is null");
        Preconditions.checkArgument(restartPositions.length() % DataUnit.INT_UNIT == 0, "restartPositions.readableBytes() must be a multiple of %s", DataUnit.INT_UNIT);
        requireNonNull(comparator, "comparator is null");

        this.data = data.input();

        this.restartPositions = restartPositions.slice();
        // 重启点的长度 / int的长度，得到重启点的个数
        restartCount = this.restartPositions.length() / DataUnit.INT_UNIT;

        this.comparator = comparator;
        // 将迭代器至于起始位置
        seekToFirst();
    }

    @Override
    public boolean hasNext()
    {
        return nextEntry != null;
    }

    @Override
    public BlockEntry peek()
    {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }
        return nextEntry;
    }

    @Override
    public BlockEntry next()
    {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }

        BlockEntry entry = nextEntry;

        if (!data.isReadable()) {
            nextEntry = null;
        }
        else {
            // 在当前位置读取Entry
            nextEntry = readEntry(data, nextEntry);
        }

        return entry;
    }

    @Override
    public void remove()
    {
        throw new UnsupportedOperationException();
    }

    /**
     * 通过设置重启点的偏移量来决定访问的数据
     */
    @Override
    public void seekToFirst()
    {
        if (restartCount > 0) {
            seekToRestartPosition(0);
        }
    }

    /**
     * 查找大于等于targetKey的Entry
     */
    @Override
    public void seek(Slice targetKey)
    {
        if (restartCount == 0) {
            return;
        }

        int left = 0;
        int right = restartCount - 1;

        // 二分查找重启点和targetKey，直到找到大于targetkey的前面一个key
        while (left < right) {
            int mid = (left + right + 1) / 2;

            seekToRestartPosition(mid);

            if (comparator.compare(nextEntry.getKey(), targetKey) < 0) {
                left = mid;
            } else {
                right = mid - 1;
            }
        }

        // 在当前重启点及后面的区域里，线性查找Entry
        for (seekToRestartPosition(left); nextEntry != null; next()) {
            if (comparator.compare(peek().getKey(), targetKey) >= 0) {
                break;
            }
        }

    }

    /**
     * 读取指定重启点的Entry，因为重启点没有压缩key的部分，所以previousEntry是null
     *
     */
    private void seekToRestartPosition(int restartPosition)
    {
        checkPositionIndex(restartPosition, restartCount, "restartPosition");

        // 根据重启点的偏移获得k-v存储区的偏移量
        int offset = restartPositions.getInt(restartPosition * DataUnit.INT_UNIT);
        data.setPosition(offset);

        // clear the entries to assure key is not prefixed
        nextEntry = null;

        // 读取Entry
        nextEntry = readEntry(data, null);
    }

    /**
     * 将缓存数据转为BlockEntry
     * 读取完这个BlockEntry之后，index会指向后面一个data的index
     */
    private static BlockEntry readEntry(SliceInput data, BlockEntry previousEntry)
    {
        requireNonNull(data, "data is null");

        // 读取Block当前key的共享前缀长度， 前缀之后的字符串长度，值的长度
        int sharedKeyLength = Coding.decodeInt(data);
        int nonSharedKeyLength = Coding.decodeInt(data);
        int valueLength = Coding.decodeInt(data);

        // 读取key
        final Slice key;
        if (sharedKeyLength > 0) {
            // 根据贡献key的长度和非共享key的长度创建新的key
            key = Slices.allocate(sharedKeyLength + nonSharedKeyLength);
            // 创建key的SliceOutput
            SliceOutput sliceOutput = key.output();
            checkState(previousEntry != null, "Entry has a shared key but no previous entry was provided");
            // 将前缀key写入到sliceOutput
            sliceOutput.writeBytes(previousEntry.getKey(), 0, sharedKeyLength);
            // 将key的后半部分写入到sliceOutput
            sliceOutput.writeBytes(data, nonSharedKeyLength);
        }
        else {
            // 没有前缀的情况下，将nonSharedKeyLength长度的数据读到key
            key = data.readSlice(nonSharedKeyLength);
        }
        // 读取value
        Slice value = data.readSlice(valueLength);

        return new BlockEntry(key, value);
    }
}
