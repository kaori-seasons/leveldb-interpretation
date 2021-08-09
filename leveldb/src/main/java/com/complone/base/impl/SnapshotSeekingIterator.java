package com.complone.base.impl;

import com.complone.base.DBIterator;
import com.complone.base.include.Slice;
import com.complone.base.utils.AbstractSeekingIterator;

import java.util.Comparator;
import java.util.Map;

/**
 * @see <<数据库系统内幕>> LSM树
 * 对memtable中的数据行进行读取操作，记录或者释放合并的层数
 */
public final class SnapshotSeekingIterator extends AbstractSeekingIterator<Slice,Slice> {

    private final DbIterator iterator; //遍历memetable的读取器

    private final SnapshotImpl snapshot; //在compact的过程当中回产生快照

    private final Comparator<Slice> userComparator; //key不一样的情况下比较key，一样的情况下意味着遇到了对象删除或者修改操作，
    // 比较SequenceNumber

    public SnapshotSeekingIterator(DbIterator iterator, SnapshotImpl snapshot, Comparator<Slice> userComparator) {
        this.iterator = iterator;
        this.snapshot = snapshot;
        this.userComparator = userComparator;
        //累计当前读取memtable的层数
        this.snapshot.getVersion().retain();
    }


    @Override
    protected void seekToFirstInternal() {
        //进入下一条数据读取
        iterator.seekToFirst();
        //删除已经读取的数据记录
        findNextUserEntry(null);

    }

    @Override
    protected void seekInternal(Slice targetKey) {

    }

    @Override
    protected Map.Entry<Slice, Slice> getNextElement() {
        return null;
    }

    private void findNextUserEntry(Slice deletedKey){
        // 是否遍历数据记录到迭代器的末尾
        if (!iterator.hasNext()){
            return;
        }

        while(iterator.hasNext()){
            //TODO 得到顺序健位
            //InternalKey internalKey = iterator.peekNext().getKey();


        }
    }


    public void close()
    {
        this.snapshot.getVersion().release();
    }

}
