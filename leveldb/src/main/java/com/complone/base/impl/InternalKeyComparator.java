package com.complone.base.impl;

import com.complone.base.table.UserComparator;

import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;

public class InternalKeyComparator
        implements Comparator<InternalKey>
{
    private final UserComparator userComparator;

    public InternalKeyComparator(UserComparator userComparator)
    {
        this.userComparator = userComparator;
    }

    public UserComparator getUserComparator()
    {
        return userComparator;
    }

    public String name()
    {
        return this.userComparator.name();
    }


    /**
     * key不一样的情况下比较key，一样的情况下意味着遇到了对象删除或者修改操作，比较SequenceNumber
     * 由此可见其排序比较依据依次是：
     * 1. 首先根据user key按升序排列
     * 2. 然后根据sequence number按降序排列
     * 3. 最后根据value type按降序排列
     *
     * 虽然比较时value type并不重要，因为sequence number是唯一的，但是直接取出8byte的sequence number | value type，
     * 然后做比较更方便，不需要再次移位提取出7byte的sequence number，又何乐而不为呢。
     * 这也是把value type安排在低7byte的好处吧，排序的两个依据就是user key和sequence number。
     * @param left
     * @param right
     * @return
     */
    @Override
    public int compare(InternalKey left, InternalKey right)
    {
        int result = userComparator.compare(left.getUserKey(), right.getUserKey());
        if (result != 0) {
            return result;
        }
        // SequenceNumber越大，说明插入时间越晚，数据就越新，如果排序的话应该排在前面
        return Long.compare(right.getSequenceNumber(), left.getSequenceNumber()); // reverse sorted version numbers
    }

    /**
     * 如果InternalKey是顺序递增的，则返回true，否则返回false
     */
    public boolean isOrdered(InternalKey... keys)
    {
        return isOrdered(Arrays.asList(keys));
    }

    /**
     * 如果InternalKey是顺序递增的，则返回true，否则返回false
     */
    public boolean isOrdered(Iterable<InternalKey> keys)
    {
        Iterator<InternalKey> iterator = keys.iterator();
        if (!iterator.hasNext()) {
            return true;
        }

        InternalKey previous = iterator.next();
        while (iterator.hasNext()) {
            InternalKey next = iterator.next();
            if (compare(previous, next) > 0) {
                return false;
            }
            previous = next;
        }
        return true;
    }
}
