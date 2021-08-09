package com.complone.base.impl;

import com.complone.base.include.Slice;
import com.complone.base.table.UserComparator;
import com.complone.base.utils.InternalTableIterator;
import com.complone.base.utils.Level0Iterator;
import com.google.common.base.Preconditions;

import java.util.*;

import static com.google.common.base.Preconditions.checkState;
import static com.complone.base.impl.SequenceNumber.MAX_SEQUENCE_NUMBER;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;

/**
 * @see <<数据库系统内幕>> LSM树的维护
 * 分层压实：RocksDB使用这一策略
 * 层级压实将磁盘驻留表划分为多个等级，0层表是指
 * 通过刷写memtable的内容创建的
 * 0层表的表数量只要超过某一个阈值，所有表将会自动合并
 * 并创建一个层级为1的新表
 */
public class Level0
        implements SeekingIterable<InternalKey, Slice>
{
    private final TableCache tableCache;
    private final InternalKeyComparator internalKeyComparator;
    private final List<FileMetaData> files;
    // NEWEST_FIRST是倒序排列的，越是新的FileMetaData，排在越前面
    public static final Comparator<FileMetaData> NEWEST_FIRST = new Comparator<FileMetaData>()
    {
        @Override
        public int compare(FileMetaData fileMetaData, FileMetaData fileMetaData1)
        {
            // 在生成metafile的时候，filenumber是递增的，越是新的sst，他的number越大，参见VersionSet的getNextFileNumber方法
            return (int) (fileMetaData1.getNumber() - fileMetaData.getNumber());
        }
    };

    public Level0(List<FileMetaData> files, TableCache tableCache, InternalKeyComparator internalKeyComparator)
    {
        requireNonNull(files, "files is null");
        requireNonNull(tableCache, "tableCache is null");
        requireNonNull(internalKeyComparator, "internalKeyComparator is null");

        this.files = new ArrayList<>(files);
        this.tableCache = tableCache;
        this.internalKeyComparator = internalKeyComparator;
    }

    public int getLevelNumber()
    {
        return 0;
    }

    public List<FileMetaData> getFiles()
    {
        return files;
    }

    @Override
    public Level0Iterator iterator()
    {
        return new Level0Iterator(tableCache, files, internalKeyComparator);
    }

    public LookupResult get(LookupKey key, ReadStats readStats)
    {
        if (files.isEmpty()) {
            return null;
        }

        List<FileMetaData> fileMetaDataList = new ArrayList<>(files.size());
        // level0 内的.sst文件，两个文件可能存在key重叠，所以需要遍历level0内的sst，找到要查找的key在sst内的所有sst
        // 如果不是level0 内的.sst文件，key不存在重叠
        for (FileMetaData fileMetaData : files) {
            if (internalKeyComparator.getUserComparator().compare(key.getUserKey(), fileMetaData.getSmallest().getUserKey()) >= 0 &&
                    internalKeyComparator.getUserComparator().compare(key.getUserKey(), fileMetaData.getLargest().getUserKey()) <= 0) {
                fileMetaDataList.add(fileMetaData);
            }
        }

        // 根据file number，将新的fileMetaData排在前面
        Collections.sort(fileMetaDataList, NEWEST_FIRST);

        readStats.clear();
        for (FileMetaData fileMetaData : fileMetaDataList) {
            // 根据fileMetaData中的file number，从tableCache中获得对应的table的iterator
            InternalTableIterator iterator = tableCache.newIterator(fileMetaData);

            // 在table中指向 >= lookup key的第一个key
            iterator.seek(key.getInternalKey());

            if (iterator.hasNext()) {
                // 解析出block中的key
                Map.Entry<InternalKey, Slice> entry = iterator.next();
                InternalKey internalKey = entry.getKey();
                Preconditions.checkState(internalKey != null, "Corrupt key for %s", key.getUserKey().toString(UTF_8));

                // 如果找到了key
                //  1. valuetype是value，那么返回LookupResult
                //  1. valuetype是delete，那么返回LookupResult
                if (key.getUserKey().equals(internalKey.getUserKey())) {
                    if (internalKey.getValueType() == ValueType.DELETION) {
                        return LookupResult.deleted(key);
                    }
                    else if (internalKey.getValueType() == ValueType.VALUE) {
                        return LookupResult.ok(key, entry.getValue());
                    }
                }
            }
            // 如果readStats中没有File信息，设置当前最新的sst文件为level0
            if (readStats.getSeekFile() == null) {

                readStats.setSeekFile(fileMetaData);
                readStats.setSeekFileLevel(0);
            }
        }

        return null;
    }

    public boolean someFileOverlapsRange(Slice smallestUserKey, Slice largestUserKey)
    {
        InternalKey smallestInternalKey = new InternalKey(smallestUserKey, MAX_SEQUENCE_NUMBER, ValueType.VALUE);
        int index = findFile(smallestInternalKey);

        UserComparator userComparator = internalKeyComparator.getUserComparator();
        return ((index < files.size()) &&
                userComparator.compare(largestUserKey, files.get(index).getLargest().getUserKey()) >= 0);
    }

    // 二分查找targetkey所在的文件
    private int findFile(InternalKey targetKey)
    {
        if (files.isEmpty()) {
            return files.size();
        }

        int left = 0;
        int right = files.size() - 1;

        while (left < right) {
            int mid = (left + right) / 2;

            if (internalKeyComparator.compare(files.get(mid).getLargest(), targetKey) < 0) {
                left = mid + 1;
            }
            else {
                right = mid;
            }
        }
        return right;
    }

    public void addFile(FileMetaData fileMetaData)
    {
        files.add(fileMetaData);
    }

    @Override
    public String toString()
    {
        final StringBuilder sb = new StringBuilder();
        sb.append("Level0");
        sb.append("{files=").append(files);
        sb.append('}');
        return sb.toString();
    }
}