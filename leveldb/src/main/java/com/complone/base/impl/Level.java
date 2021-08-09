package com.complone.base.impl;

import com.google.common.collect.Lists;
import com.complone.base.include.Slice;
import com.complone.base.table.UserComparator;
import com.complone.base.utils.InternalTableIterator;
import com.complone.base.utils.LevelIterator;

import java.util.*;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.complone.base.impl.SequenceNumber.MAX_SEQUENCE_NUMBER;
import static com.complone.base.impl.ValueType.VALUE;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;

public class Level
        implements SeekingIterable<InternalKey, Slice>
{
    private final int levelNumber;
    private final TableCache tableCache;
    private final InternalKeyComparator internalKeyComparator;
    private final List<FileMetaData> files;

    public Level(int levelNumber, List<FileMetaData> files, TableCache tableCache, InternalKeyComparator internalKeyComparator)
    {
        checkArgument(levelNumber >= 0, "levelNumber is negative");
        requireNonNull(files, "files is null");
        requireNonNull(tableCache, "tableCache is null");
        requireNonNull(internalKeyComparator, "internalKeyComparator is null");

        this.files = new ArrayList<>(files);
        this.tableCache = tableCache;
        this.internalKeyComparator = internalKeyComparator;
        checkArgument(levelNumber >= 0, "levelNumber is negative");
        this.levelNumber = levelNumber;
    }

    public int getLevelNumber()
    {
        return levelNumber;
    }

    public List<FileMetaData> getFiles()
    {
        return files;
    }

    @Override
    public LevelIterator iterator()
    {
        return createLevelConcatIterator(tableCache, files, internalKeyComparator);
    }

    public static LevelIterator createLevelConcatIterator(TableCache tableCache, List<FileMetaData> files, InternalKeyComparator internalKeyComparator)
    {
        return new LevelIterator(tableCache, files, internalKeyComparator);
    }

    public LookupResult get(LookupKey key, ReadStats readStats)
    {
        if (files.isEmpty()) {
            return null;
        }

        List<FileMetaData> fileMetaDataList = new ArrayList<>(files.size());
        // level0 内的.sst文件，两个文件可能存在key重叠，所以需要遍历level0内的sst，找到要查找的key在sst内的所有sst
        // 如果不是level0 内的.sst文件，key不存在重叠，就可以直接二分
        if (levelNumber == 0) {
            for (FileMetaData fileMetaData : files) {
                if (internalKeyComparator.getUserComparator().compare(key.getUserKey(), fileMetaData.getSmallest().getUserKey()) >= 0 &&
                        internalKeyComparator.getUserComparator().compare(key.getUserKey(), fileMetaData.getLargest().getUserKey()) <= 0) {
                    fileMetaDataList.add(fileMetaData);
                }
            }
        }
        else {
            // 二分查找最小的 key >= ikey的文件
            int index = ceilingEntryIndex(Lists.transform(files, FileMetaData::getLargest), key.getInternalKey(), internalKeyComparator);

            // 如果已经找到了文件最后，都没找到，说明sstable中不包含key
            if (index >= files.size()) {
                return null;
            }

            // 验证文件的最小key是不是大于key
            FileMetaData fileMetaData = files.get(index);
            if (internalKeyComparator.getUserComparator().compare(key.getUserKey(), fileMetaData.getSmallest().getUserKey()) < 0) {
                return null;
            }

            // 将该文件添加到带查找列表
            fileMetaDataList.add(fileMetaData);
        }

        FileMetaData lastFileRead = null;
        int lastFileReadLevel = -1;
        readStats.clear();
        for (FileMetaData fileMetaData : fileMetaDataList) {
            if (lastFileRead != null && readStats.getSeekFile() == null) {
                // 记录第一个文件的信息
                readStats.setSeekFile(lastFileRead);
                readStats.setSeekFileLevel(lastFileReadLevel);
            }

            lastFileRead = fileMetaData;
            lastFileReadLevel = levelNumber;

            // 根据fileMetaData中的file number，从tableCache中获得对应的table的iterator
            InternalTableIterator iterator = tableCache.newIterator(fileMetaData);

            // 在table中指向 >= lookup key的第一个key
            iterator.seek(key.getInternalKey());

            if (iterator.hasNext()) {
                // 解析出block中的key
                Map.Entry<InternalKey, Slice> entry = iterator.next();
                InternalKey internalKey = entry.getKey();
                checkState(internalKey != null, "Corrupt key for %s", key.getUserKey().toString(UTF_8));

                // 如果找到了key
                //  1. valuetype是value，那么返回LookupResult
                //  1. valuetype是delete，那么返回LookupResult
                if (key.getUserKey().equals(internalKey.getUserKey())) {
                    if (internalKey.getValueType() == ValueType.DELETION) {
                        return LookupResult.deleted(key);
                    }
                    else if (internalKey.getValueType() == VALUE) {
                        return LookupResult.ok(key, entry.getValue());
                    }
                }
            }
        }

        return null;
    }

    private static <T> int ceilingEntryIndex(List<T> list, T key, Comparator<T> comparator)
    {
        // 如果搜索键包含在列表中，则返回搜索键的索引；否则返回 (-(插入点) - 1)。
        // 插入点：被定义为将键插入列表的那一点，即第一个大于此键的元素索引；
        int insertionPoint = Collections.binarySearch(list, key, comparator);
        if (insertionPoint < 0) {
            insertionPoint = -(insertionPoint + 1);
        }
        return insertionPoint;
    }

    public boolean someFileOverlapsRange(Slice smallestUserKey, Slice largestUserKey)
    {
        InternalKey smallestInternalKey = new InternalKey(smallestUserKey, MAX_SEQUENCE_NUMBER, VALUE);
        int index = findFile(smallestInternalKey);

        UserComparator userComparator = internalKeyComparator.getUserComparator();
        return ((index < files.size()) &&
                userComparator.compare(largestUserKey, files.get(index).getLargest().getUserKey()) >= 0);
    }

    private int findFile(InternalKey targetKey)
    {
        if (files.isEmpty()) {
            return files.size();
        }

        // todo replace with Collections.binarySearch
        int left = 0;
        int right = files.size() - 1;

        // binary search restart positions to find the restart position immediately before the targetKey
        while (left < right) {
            int mid = (left + right) / 2;

            if (internalKeyComparator.compare(files.get(mid).getLargest(), targetKey) < 0) {
                // Key at "mid.largest" is < "target".  Therefore all
                // files at or before "mid" are uninteresting.
                left = mid + 1;
            }
            else {
                // Key at "mid.largest" is >= "target".  Therefore all files
                // after "mid" are uninteresting.
                right = mid;
            }
        }
        return right;
    }

    public void addFile(FileMetaData fileMetaData)
    {
        // todo remove mutation
        files.add(fileMetaData);
    }

    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder();
        sb.append("Level");
        sb.append("{levelNumber=").append(levelNumber);
        sb.append(", files=").append(files);
        sb.append('}');
        return sb.toString();
    }
}