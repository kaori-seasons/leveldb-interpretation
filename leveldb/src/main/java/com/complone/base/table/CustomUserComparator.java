package com.complone.base.table;

import com.complone.base.DBComparator;
import com.complone.base.include.Slice;

public class CustomUserComparator
        implements UserComparator
{
    private final DBComparator comparator;

    public CustomUserComparator(DBComparator comparator)
    {
        this.comparator = comparator;
    }

    @Override
    public String name()
    {
        return comparator.name();
    }

    @Override
    public Slice findShortestSeparator(Slice start, Slice limit)
    {
        return new Slice(comparator.findShortestSeparator(start.getBytes(), limit.getBytes()));
    }

    @Override
    public Slice findShortSuccessor(Slice key)
    {
        return new Slice(comparator.findShortSuccessor(key.getBytes()));
    }

    @Override
    public int compare(Slice o1, Slice o2)
    {
        return comparator.compare(o1.getBytes(), o2.getBytes());
    }
}
