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

import com.complone.base.include.DynamicSliceOutput;
import com.complone.base.include.Slice;
import com.complone.base.utils.Coding;
import com.complone.base.utils.DataUnit;
import com.google.common.primitives.Ints;
import com.complone.base.utils.IntVector;

import java.util.Comparator;

import static com.google.common.base.Preconditions.*;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;


public class BlockBuilder {
    // blockRestartInterval记录每隔多少个数据，会有一个不压缩的前缀
    private final int blockRestartInterval;
    // 重启点
    private final IntVector restartPositions;
    private final Comparator<Slice> comparator;
    // 存入的key-value对的个数
    private int entryCount;
    // 重启点个数
    private int restartBlockEntryCount;
    // 是否构建完成
    private boolean finished;
    // block的内容
    private final DynamicSliceOutput block;
    // 记录最后添加的key
    private Slice lastKey;

    public BlockBuilder(int estimatedSize, int blockRestartInterval, Comparator<Slice> comparator)
    {
        checkArgument(estimatedSize >= 0, "estimatedSize is negative");
        checkArgument(blockRestartInterval >= 0, "blockRestartInterval is negative");
        requireNonNull(comparator, "comparator is null");

        this.block = new DynamicSliceOutput(estimatedSize);
        this.blockRestartInterval = blockRestartInterval;
        this.comparator = comparator;

        restartPositions = new IntVector(32);
        // 第一个重启点必须是0
        restartPositions.add(0);
    }
    // 重设内容，通常在Finish之后调用，来构建新的block
    public void reset()
    {
        block.reset();
        entryCount = 0;
        restartPositions.clear();
        // 第一个重启点必须是0
        restartPositions.add(0);
        restartBlockEntryCount = 0;
        lastKey = null;
        finished = false;
    }
    public int getEntryCount()
    {
        return entryCount;
    }
    // 没有entry则返回true
    public boolean isEmpty()
    {
        return entryCount == 0;
    }

    // 返回正在构建block的未压缩大小（估计值）
    public int currentSizeEstimate()
    {
        // 如果构建已经结束，返回block的size
        if (finished) {
            return block.size();
        }

        // 如果block中还没有数据，则只有int的长度，这个int记录了重启点的个数，会在一开始被初始化为0
        if (block.size() == 0) {
            return DataUnit.INT_UNIT;
        }

        return block.size() +                              // k/v存储区
                restartPositions.size() * DataUnit.INT_UNIT +    // 重启点存储区
                DataUnit.INT_UNIT;                               // 一个int标识了重启点的个数
    }

    public void add(BlockEntry blockEntry)
    {
        requireNonNull(blockEntry, "blockEntry is null");
        add(blockEntry.getKey(), blockEntry.getValue());
    }
    //添加k/v，要求：Reset()之后没有调用过Finish()；Key > 任何已加入的key
    public void add(Slice key, Slice value)
    {
        requireNonNull(key, "key is null");
        requireNonNull(value, "value is null");
        checkState(!finished, "block is finished");
        // 判断block中存储的经过压缩的key是否大于等于要求的interval，如果大于等于，则抛出异常
        checkPositionIndex(restartBlockEntryCount, blockRestartInterval);
        //如果key小于最后一个key，则抛出IllegalArgumentException异常
        checkArgument(lastKey == null || comparator.compare(key, lastKey) > 0, "key must be greater than last key");

        int sharedKeyBytes = 0;
        if (restartBlockEntryCount < blockRestartInterval) {
            sharedKeyBytes = calculateSharedBytes(key, lastKey);
        }
        else {
            // 如果压缩key的数据已经等于interval了，则需要重新开始计数
            // 重置重启点为当前index，count为0
            restartPositions.add(block.size());
            restartBlockEntryCount = 0;
        }

        int nonSharedKeyBytes = key.length() - sharedKeyBytes;

        // 根据下面的规则写入shared_bytes | unshared_bytes | value_length
        // shared_bytes | unshared_bytes | value_length | key_delta | value
        Coding.encodeInt(sharedKeyBytes, block);
        Coding.encodeInt(nonSharedKeyBytes, block);
        Coding.encodeInt(value.length(), block);

        // 写入key
        // 从sharedKeyBytes位开始写，共写nonSharedKeyBytes位，将key写入block
        block.writeBytes(key, sharedKeyBytes, nonSharedKeyBytes);

        // 写入value
        block.writeBytes(value, 0, value.length());

        // 更新lastkey
        lastKey = key;

        // 更新状态
        entryCount++;
        restartBlockEntryCount++;
    }

    /**
     * 计算公共前缀的长度
     */
    public static int calculateSharedBytes(Slice leftKey, Slice rightKey)
    {
        int sharedKeyBytes = 0;

        if (leftKey != null && rightKey != null) {
            int minSharedKeyBytes = Ints.min(leftKey.length(), rightKey.length());
            while (sharedKeyBytes < minSharedKeyBytes && leftKey.getByte(sharedKeyBytes) == rightKey.getByte(sharedKeyBytes)) {
                sharedKeyBytes++;
            }
        }

        return sharedKeyBytes;
    }
    /**
     * 结束构建block，并返回指向block内容的指针
     * @return Slice的生存周期：Builder的生存周期，or直到Reset()被调用
     */
    public Slice finish()
    {
        if (!finished) {
            finished = true;

            if (entryCount > 0) {
                // 把重启点的数据写入block中，写入重启点的个数
                restartPositions.write(block);
                block.writeInt(restartPositions.size());
            }
            else {
                block.writeInt(0);
            }
        }
        // 返回数据
        return block.slice();
    }
}
