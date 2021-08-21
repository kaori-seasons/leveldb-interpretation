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

import com.complone.base.include.Slice;
import com.complone.base.table.Block;
import com.complone.base.table.BlockIterator;
import com.complone.base.table.Table;

import java.util.Map;

public final class TableIterator
        extends AbstractSeekingIterator<Slice, Slice>
{
    private final Table table;
    /**
     * 各种Block的存储格式都是相同的，但是各自block data存储的k/v又互不相同，于是我们就需要一个途径，
     * 能够在使用同一个方式遍历不同的block时，又能解析这些k/v。
     */
    private final BlockIterator blockIterator;
    /**
     * 遍历block data的迭代器
     */
    private BlockIterator current;

    public TableIterator(Table table, BlockIterator blockIterator)
    {
        this.table = table;
        this.blockIterator = blockIterator;
        current = null;
    }

    @Override
    protected void seekToFirstInternal()
    {
        // 重置index到data block的起始位置
        blockIterator.seekToFirst();
        current = null;
    }

    //TODO 参考此处遍历数据块的方式找到对应的Key
    // 第一阶段先这样基于重入锁+slice切片的方式保证顺序性
    // 第二阶段 在扫描数据块之前 参考DbImpl#Line 114
    // 在中间层加一层表缓存,采用FileChannel或者mmap的方式
    // 参考 MMapTable.java 以及FileChannelTable.java
    // 间接操作磁盘(不违反比赛规则)
    @Override
    protected void seekInternal(Slice targetKey)
    {
        // 这里并不是精确的定位，而是在Table中找到第一个>=指定key的k/v对
        blockIterator.seek(targetKey);

        // 如果iterator没有next，那么key不包含在iterator中
        if (blockIterator.hasNext()) {
            // 找到current的迭代器
            current = getNextBlock();
            current.seek(targetKey);
        }
        else {
            current = null;
        }
    }

    @Override
    protected Map.Entry<Slice, Slice> getNextElement()
    {
        boolean currentHasNext = false;
        while (true) {
            if (current != null) {
                currentHasNext = current.hasNext();
            }
            if (!(currentHasNext)) {
                if (blockIterator.hasNext()) {
                    current = getNextBlock();
                }
                else {
                    break;
                }
            }
            else {
                break;
            }
        }
        if (currentHasNext) {
            return current.next();
        }
        else {
            // set current to empty iterator to avoid extra calls to user iterators
            current = null;
            return null;
        }
    }

    private BlockIterator getNextBlock()
    {
        Slice blockHandle = blockIterator.next().getValue();
        Block dataBlock = table.openBlock(blockHandle);
        return dataBlock.iterator();
    }

    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder();
        sb.append("ConcatenatingIterator");
        sb.append("{blockIterator=").append(blockIterator);
        sb.append(", current=").append(current);
        sb.append('}');
        return sb.toString();
    }
}
