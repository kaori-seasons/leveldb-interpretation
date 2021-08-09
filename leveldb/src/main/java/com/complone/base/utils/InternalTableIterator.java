package com.complone.base.utils;

import com.complone.base.impl.InternalKey;
import com.complone.base.include.Slice;
import com.google.common.collect.Maps;

import java.util.Map;

public class InternalTableIterator
        extends AbstractSeekingIterator<InternalKey, Slice>
        implements InternalIterator
{
    private final TableIterator tableIterator;

    public InternalTableIterator(TableIterator tableIterator)
    {
        this.tableIterator = tableIterator;
    }

    @Override
    protected void seekToFirstInternal()
    {
        tableIterator.seekToFirst();
    }

    @Override
    public void seekInternal(InternalKey targetKey)
    {
        tableIterator.seek(targetKey.encode());
    }

    @Override
    protected Map.Entry<InternalKey, Slice> getNextElement()
    {
        if (tableIterator.hasNext()) {
            Map.Entry<Slice, Slice> next = tableIterator.next();
            // guava 的 Maps.immutableEntry方法用于创建单个键值对
            return Maps.immutableEntry(new InternalKey(next.getKey()), next.getValue());
        }
        return null;
    }

    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder();
        sb.append("InternalTableIterator");
        sb.append("{fromIterator=").append(tableIterator);
        sb.append('}');
        return sb.toString();
    }
}
