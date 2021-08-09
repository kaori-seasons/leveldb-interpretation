package com.complone.base.utils;

import com.complone.base.impl.InternalKey;
import com.complone.base.include.Slice;

import java.util.*;

/**
 * MergingIterator主要是用于合并的。
 */
public final class MergingIterator
        extends AbstractSeekingIterator<InternalKey, Slice>
{
    private final List<? extends InternalIterator> levels;
    private final PriorityQueue<ComparableIterator> priorityQueue;
    private final Comparator<InternalKey> comparator;

    public MergingIterator(List<? extends InternalIterator> levels, Comparator<InternalKey> comparator)
    {
        this.levels = levels;
        this.comparator = comparator;

        this.priorityQueue = new PriorityQueue<>(levels.size() + 1);
        resetPriorityQueue(comparator);
    }

    @Override
    protected void seekToFirstInternal()
    {
        // 移到该层文件头的位置

        for (InternalIterator level : levels) {
            level.seekToFirst();
        }
        resetPriorityQueue(comparator);
    }

    @Override
    protected void seekInternal(InternalKey targetKey)
    {
        for (InternalIterator level : levels) {
            level.seek(targetKey);
        }
        resetPriorityQueue(comparator);
    }

    // 重置每一层level的元素到Comparator
    // 注意这里从level 1 开始
    private void resetPriorityQueue(Comparator<InternalKey> comparator)
    {
        int i = 1;
        for (InternalIterator level : levels) {
            if (level.hasNext()) {
                // level.next()指向的是MemTableIterator中的key-value
                // ComparableIterator的排序是按照nextElement.key 和 ordinal排序的，越新的数据ordinal越小
                // 用一个小根堆实现了多层元素的排序，妙啊！
                priorityQueue.add(new ComparableIterator(level, comparator, i++, level.next()));
            }
        }
    }
    // NextElement一定是这几层中key最小的元素
    @Override
    protected Map.Entry<InternalKey, Slice> getNextElement()
    {
        Map.Entry<InternalKey, Slice> result = null;
        ComparableIterator nextIterator = priorityQueue.poll();
        if (nextIterator != null) {
            result = nextIterator.next();
            if (nextIterator.hasNext()) {
                priorityQueue.add(nextIterator);
            }
        }
        return result;
    }

    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder();
        sb.append("MergingIterator");
        sb.append("{levels=").append(levels);
        sb.append(", comparator=").append(comparator);
        sb.append('}');
        return sb.toString();
    }

    private static class ComparableIterator
            implements Iterator<Map.Entry<InternalKey, Slice>>, Comparable<ComparableIterator>
    {
        private final InternalIterator iterator;
        private final Comparator<InternalKey> comparator;
        private final int ordinal;
        private Map.Entry<InternalKey, Slice> nextElement;

        private ComparableIterator(InternalIterator iterator, Comparator<InternalKey> comparator, int ordinal, Map.Entry<InternalKey, Slice> nextElement)
        {
            this.iterator = iterator;
            this.comparator = comparator;
            this.ordinal = ordinal;
            this.nextElement = nextElement;
        }

        @Override
        public boolean hasNext()
        {
            return nextElement != null;
        }

        @Override
        public Map.Entry<InternalKey, Slice> next()
        {
            if (nextElement == null) {
                throw new NoSuchElementException();
            }

            Map.Entry<InternalKey, Slice> result = nextElement;
            if (iterator.hasNext()) {
                nextElement = iterator.next();
            }
            else {
                nextElement = null;
            }
            return result;
        }

        @Override
        public void remove()
        {
            throw new UnsupportedOperationException();
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

            ComparableIterator comparableIterator = (ComparableIterator) o;

            if (ordinal != comparableIterator.ordinal) {
                return false;
            }
            if (nextElement != null ? !nextElement.equals(comparableIterator.nextElement) : comparableIterator.nextElement != null) {
                return false;
            }

            return true;
        }

        @Override
        public int hashCode()
        {
            int result = ordinal;
            result = 31 * result + (nextElement != null ? nextElement.hashCode() : 0);
            return result;
        }

        // ComparableIterator的排序是按照nextElement.key 和 ordinal排序的，越新的数据ordinal越小
        @Override
        public int compareTo(ComparableIterator that)
        {
            int result = comparator.compare(this.nextElement.getKey(), that.nextElement.getKey());
            if (result == 0) {
                result = Integer.compare(this.ordinal, that.ordinal);
            }
            return result;
        }
    }
}