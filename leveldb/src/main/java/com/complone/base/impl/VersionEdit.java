package com.complone.base.impl;

import com.complone.base.include.Slice;
import com.complone.base.include.SliceInput;
import com.complone.base.utils.Coding;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Multimap;
import com.complone.base.include.DynamicSliceOutput;

import java.util.Map;
import java.util.TreeMap;

/**
 * VersionEdit记录了Version之间的变化，相当于delta增量，表示又增加了多少文件，删除了文件。
 * 也就是说：Version0 + VersionEdit --> Version1。
 * 每次文件有变动时，leveldb就把变动记录到一个VersionEdit变量中，然后通过VersionEdit把变动应用到current version上，
 * 并把current version的快照，也就是db元信息保存到MANIFEST文件中。
 * 另外，MANIFEST文件组织是以VersionEdit的形式写入的，它本身是一个log文件格式，采用log::Writer/Reader的方式读写，
 * 一个VersionEdit就是一条log record。
 *
 * LevelDB中对Manifest的Decode/Encode是通过类VersionEdit完成的，Manifest文件保存了LevelDB的管理元信息。
 * 每一次compaction，都好比是生成了一个新的DB版本，对应的Manifest则保存着这个版本的DB元信息。
 * VersionEdit并不操作文件，只是为Manifest文件读写准备好数据、从读取的数据中解析出DB元信息。
 * VersionEdit有两个作用：
 * 1 当版本间有增量变动时，VersionEdit记录了这种变动；
 * 2 写入到MANIFEST时，先将current version的db元信息保存到一个VersionEdit中，然后在组织成一个log record写入文件；
 */
public class VersionEdit
{
    private String comparatorName;
    // 日志编号
    private Long logNumber;
    // 前一个日志编号
    private Long previousLogNumber;
    // 下一个文件编号
    private Long nextFileNumber;
    // 上一个seq
    private Long lastSequenceNumber;
    // 下面的这些key都是level
    // compact点
    private final Map<Integer, InternalKey> compactPointers = new TreeMap<>();
    // 新文件集合
    private final Multimap<Integer, FileMetaData> newFiles = ArrayListMultimap.create();
    // 删除文件集合
    private final Multimap<Integer, Long> deletedFiles = ArrayListMultimap.create();

    public VersionEdit()
    {
    }

    public VersionEdit(Slice slice)
    {
        SliceInput sliceInput = slice.input();
        while (sliceInput.isReadable()) {
            int i = Coding.decodeInt(sliceInput);
            VersionEditTag tag = VersionEditTag.getValueTypeByPersistentId(i);
            tag.readValue(sliceInput, this);
        }
    }

    public String getComparatorName()
    {
        return comparatorName;
    }

    public void setComparatorName(String comparatorName)
    {
        this.comparatorName = comparatorName;
    }

    public Long getLogNumber()
    {
        return logNumber;
    }

    public void setLogNumber(long logNumber)
    {
        this.logNumber = logNumber;
    }

    public Long getNextFileNumber()
    {
        return nextFileNumber;
    }

    public void setNextFileNumber(long nextFileNumber)
    {
        this.nextFileNumber = nextFileNumber;
    }

    public Long getPreviousLogNumber()
    {
        return previousLogNumber;
    }

    public void setPreviousLogNumber(long previousLogNumber)
    {
        this.previousLogNumber = previousLogNumber;
    }

    public Long getLastSequenceNumber()
    {
        return lastSequenceNumber;
    }

    public void setLastSequenceNumber(long lastSequenceNumber)
    {
        this.lastSequenceNumber = lastSequenceNumber;
    }

    public Map<Integer, InternalKey> getCompactPointers()
    {
        return ImmutableMap.copyOf(compactPointers);
    }

    // 把{level, key}指定的compact点加入到compact_pointers_中
    public void setCompactPointer(int level, InternalKey key)
    {
        compactPointers.put(level, key);
    }

    public void setCompactPointers(Map<Integer, InternalKey> compactPointers)
    {
        this.compactPointers.putAll(compactPointers);
    }

    public Multimap<Integer, FileMetaData> getNewFiles()
    {
        return ImmutableMultimap.copyOf(newFiles);
    }


    /**
     * 添加sstable文件信息，要求：DB元信息还没有写入磁盘Manifest文件
     * @param level .sst文件层次；
     * @param fileNumber 文件编号-用作文件名
     * @param fileSize 文件大小
     * @param smallest sst文件包含k/v对的最小key
     * @param largest sst文件包含k/v对的最大key
     */
    public void addFile(int level, long fileNumber,
                        long fileSize,
                        InternalKey smallest,
                        InternalKey largest)
    {
        FileMetaData fileMetaData = new FileMetaData(fileNumber, fileSize, smallest, largest);
        addFile(level, fileMetaData);
    }

    public void addFile(int level, FileMetaData fileMetaData)
    {
        newFiles.put(level, fileMetaData);
    }

    public void addFiles(Multimap<Integer, FileMetaData> files)
    {
        newFiles.putAll(files);
    }

    public Multimap<Integer, Long> getDeletedFiles()
    {
        return ImmutableMultimap.copyOf(deletedFiles);
    }

    // 从指定的level删除文件
    public void deleteFile(int level, long fileNumber)
    {
        deletedFiles.put(level, fileNumber);
    }

    // 将信息Encode到一个string中
    public Slice encode()
    {
        DynamicSliceOutput dynamicSliceOutput = new DynamicSliceOutput(4096);
        for (VersionEditTag versionEditTag : VersionEditTag.values()) {
            versionEditTag.writeValue(dynamicSliceOutput, this);
        }
        return dynamicSliceOutput.slice();
    }

    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder();
        sb.append("VersionEdit");
        sb.append("{comparatorName='").append(comparatorName).append('\'');
        sb.append(", logNumber=").append(logNumber);
        sb.append(", previousLogNumber=").append(previousLogNumber);
        sb.append(", lastSequenceNumber=").append(lastSequenceNumber);
        sb.append(", compactPointers=").append(compactPointers);
        sb.append(", newFiles=").append(newFiles);
        sb.append(", deletedFiles=").append(deletedFiles);
        sb.append('}');
        return sb.toString();
    }
}