package com.complone.base.impl;

import com.complone.base.db.MemTable.MemTableIterator;
import com.complone.base.include.Slice;
import com.complone.base.utils.AbstractSeekingIterator;
import com.complone.base.utils.InternalIterator;
import com.complone.base.utils.InternalTableIterator;
import com.complone.base.utils.LevelIterator;


import java.util.*;

/**
 * 数据行迭代器
 */
public final class DbIterator extends
        AbstractSeekingIterator<InternalKey, Slice> implements InternalIterator {

    /**当前读取的memtable数据行 **/
    private final MemTableIterator memTableIterator;

    /**多层compact之后的memtable数据行 **/
    private final MemTableIterator immutableMemTableIterator;

    /** 待刷写memtable的多层表**/
    private final List<InternalTableIterator> level0Files;

    /**对每层表进行合并操作的数据迭代器 **/
    private final List<LevelIterator> levels;

    private int heapSize;



    /**key不一样的时候比较key，如果遇到删除或者修改操作需要对序列号进行排序 **/
    private Comparator<InternalKey> comparator;


    public DbIterator(MemTableIterator memTableIterator,
                      MemTableIterator immutableMemTableIterator,
                      List<InternalTableIterator> level0Files,
                      List<LevelIterator> levels,
                      Comparator<InternalKey> comparator){
        this.memTableIterator = memTableIterator;
        this.immutableMemTableIterator = immutableMemTableIterator;
        this.level0Files = level0Files;
        this.levels = levels;
        this.comparator = comparator;

    }

    @Override
    protected void seekToFirstInternal() {
        if (memTableIterator!=null){
            memTableIterator.seekToFirst();
        }

        if (immutableMemTableIterator!=null){
            immutableMemTableIterator.seekToFirst();
        }

        for(InternalTableIterator level0File: level0Files){
            level0File.seekToFirst();
        }

        for (LevelIterator level: levels){
            level.seekToFirst();
        }
    }

    @Override
    protected void seekInternal(InternalKey targetKey) {

    }

    @Override
    protected Map.Entry<InternalKey, Slice> getNextElement() {
        return null;
    }

    /**
     * 由于磁盘驻留表上的内容是经过排序的，所以需要使用多路归并
     * 这里使用一个优先队列(最小堆)实现多路归并
     * 该队列保存N个元素(N为迭代器数量)，对内容排序后返回下一个
     * 最小元素，每个迭代器的头放入队列，队列的头部元素就是最小值
     * 目前有三个数据源 两个磁盘驻留表和一个memtable
     * 一般是用迭代器和游标来遍历内容，此游标保存着上次消耗数据的偏移量
     * 可以通过检查迭代是否完成，也可以用来抽取下一个数据记录
     */
    private void resetPriorityQueue(){
        int i =0 ;
        heapSize = 0;
        if (memTableIterator!=null && memTableIterator.hasNext()){
            //TODO 基于优先队列实现小顶堆
//            heapAdd();
        }

        if (immutableMemTableIterator!=null && immutableMemTableIterator.hasNext()){
//            heapAdd()
        }

        for (InternalTableIterator level0File: level0Files){
            if (level0File.hasNext()) {
//                heapAdd();
            }
        }
        for (LevelIterator level: levels){
            if (level.hasNext()){
                //heapAdd();
            }
        }

    }

    /**
     * 比较生成的序列号
     */
    private static class ComparableIterator implements Iterator<Map.Entry<InternalKey,Slice>>, Comparable<ComparableIterator>{

        //重置到写入block的位点
        private final SeekingIterator<InternalKey,Slice> iterator;
        //有序key的比较器
        private final Comparator<InternalKey> comparator;

        private final int ordinal;
        //从ssttable文件中读取的下一个元素
        private Map.Entry<InternalKey,Slice> nextElement;

        private ComparableIterator(SeekingIterator<InternalKey,Slice> iterator,
                                   Comparator<InternalKey> comparator, int ordinal, Map.Entry<InternalKey,Slice> nextElement ){
           this.iterator = iterator;
           this.comparator = comparator;
           this.ordinal = ordinal;
           this.nextElement = nextElement;

        }


        @Override
        public int compareTo(ComparableIterator o) {

            int result = comparator.compare(this.nextElement.getKey(),o.nextElement.getKey());
            if (result == 0){
                result = Integer.compare(this.ordinal,o.ordinal);
            }

            return result;
        }

        @Override
        public boolean hasNext() {
            return nextElement!=null;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            ComparableIterator comparableIterator = (ComparableIterator) o;
            //因为InternalKey是一个序列号的key，所以每个key都有一个序号,需要进行引用比较
            if (ordinal != comparableIterator.ordinal){
                return false;
            }

            //迭代下一个元素
            if (nextElement != null ? !nextElement.equals(comparableIterator.nextElement): comparableIterator.nextElement!=null){
                return false;
            }
            return true;

        }

        @Override
        public int hashCode() {
            return Objects.hash(iterator, comparator, ordinal, nextElement);
        }

        @Override
        public Map.Entry<InternalKey, Slice> next() {


            if (nextElement == null){
                throw new NoSuchElementException();
            }

            Map.Entry<InternalKey,Slice> result = nextElement;
            if (iterator.hasNext()){
                nextElement = iterator.next();
            }else{
                nextElement = null;
            }
            return result;

        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }
    }
}
