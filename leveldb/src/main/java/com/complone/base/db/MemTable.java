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

import com.complone.base.impl.*;
import com.complone.base.include.Slice;
import com.complone.base.utils.DataUnit;
import com.google.common.collect.Iterators;
import com.google.common.collect.PeekingIterator;
import com.complone.base.utils.InternalIterator;
import java.util.Map;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;

import static java.util.Objects.requireNonNull;


/**
 * leveldb之所以有level这个单词就是因为数据存储分层管理，而日志和内存表（memtable）处于第0层
 * 在Leveldb中，所有内存中的KV数据都存储在Memtable中，物理disk则存储在SSTable中。
 * 在系统运行过程中，如果Memtable中的数据占用内存到达指定值(Options.write_buffer_size)，
 * 则Leveldb就自动将Memtable转换为Immutable Memtable，并自动生成新的Memtable，也就是Copy-On-Write机制了。
 * Immutable Memtable则被新的线程Dump到磁盘中，Dump结束则该Immutable Memtable就可以释放了。因名知意，Immutable Memtable是只读的。
 *
 * 所以可见，最新的数据都是存储在Memtable中的，Immutable Memtable和物理SSTable则是某个时点的数据。
 * Memtable提供了写入KV记录，删除以及读取KV记录的接口，
 * 但是事实上Memtable并不执行真正的删除操作,删除某个Key的Value在Memtable内是作为插入一条记录实施的，但是会打上一个Key的删除标记，
 * 真正的删除操作在后面的 Compaction过程中，lazy delete。
 * ConcurrentSkipListMap是MemTable的核心数据结构，memtable的KV数据都存储在ConcurrentSkipListMap中。
 */
public class MemTable
        implements SeekingIterable<InternalKey, Slice>
{
    private final ConcurrentSkipListMap<InternalKey, Slice> table;
    private final AtomicLong approximateMemoryUsage = new AtomicLong();

    public MemTable(InternalKeyComparator internalKeyComparator)
    {
        table = new ConcurrentSkipListMap<>(internalKeyComparator);
    }

    public boolean isEmpty()
    {
        return table.isEmpty();
    }

    public long approximateMemoryUsage()
    {
        return approximateMemoryUsage.get();
    }

    public void add(long sequenceNumber, ValueType valueType, Slice key, Slice value)
    {
        requireNonNull(valueType, "valueType is null");
        requireNonNull(key, "key is null");

        InternalKey internalKey = new InternalKey(key, sequenceNumber, valueType);
        table.put(internalKey, value);

        // 将在函数的参数中传递的值添加到先前的值,并返回数据类型为long的新更新值。
        approximateMemoryUsage.addAndGet(key.length() + DataUnit.LONG_UNIT + value.length());
    }

    // Memtable的查询接口传入的是LookupKey，它也是由User Key和Sequence Number组合而成的
    public LookupResult get(LookupKey key)
    {
        requireNonNull(key, "key is null");

        InternalKey internalKey = key.getInternalKey();
        // 返回与该键至少大于或等于给定键,如果不存在这样的键的键 - 值映射,则返回null相关联。
        Map.Entry<InternalKey, Slice> entry = table.ceilingEntry(internalKey);
        if (entry == null) {
            return null;
        }

        InternalKey entryKey = entry.getKey();
        if (entryKey.getUserKey().equals(key.getUserKey())) {
            if (entryKey.getValueType() == ValueType.DELETION) {
                return LookupResult.deleted(key);
            }
            else {
                return LookupResult.ok(key, entry.getValue());
            }
        }
        return null;
    }

    /**
     * 可以遍历访问table的内部数据，很好的设计思想，这种方式隐藏了table的内部实现。
     * 外部调用者必须保证使用Iterator访问Memtable的时候该Memtable是live的。
     * @return 返回一个迭代器
     */
    @Override
    public MemTableIterator iterator()
    {
        return new MemTableIterator();
    }

    public class MemTableIterator
            implements InternalIterator
    {
        // PeekingIterator是自定义的迭代器，是对顶层迭代器Iterator的封装。
        private PeekingIterator<Map.Entry<InternalKey, Slice>> iterator;

        public MemTableIterator()
        {
            iterator = Iterators.peekingIterator(table.entrySet().iterator());
        }

        @Override
        public boolean hasNext()
        {
            return iterator.hasNext();
        }

        @Override
        public void seekToFirst()
        {
            iterator = Iterators.peekingIterator(table.entrySet().iterator());
        }

        @Override
        public void seek(InternalKey targetKey)
        {
            iterator = Iterators.peekingIterator(table.tailMap(targetKey).entrySet().iterator());
        }

        @Override
        public InternalEntry peek()
        {
            Map.Entry<InternalKey, Slice> entry = iterator.peek();
            return new InternalEntry(entry.getKey(), entry.getValue());
        }

        @Override
        public InternalEntry next()
        {
            Map.Entry<InternalKey, Slice> entry = iterator.next();
            return new InternalEntry(entry.getKey(), entry.getValue());
        }

        @Override
        public void remove()
        {
            throw new UnsupportedOperationException();
        }
    }
}
