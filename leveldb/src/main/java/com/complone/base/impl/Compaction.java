package com.complone.base.impl;

import com.google.common.collect.ImmutableList;
import com.complone.base.include.Slice;
import com.complone.base.table.UserComparator;

import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static com.complone.base.impl.DbConstants.NUM_LEVELS;
import static com.complone.base.impl.VersionSet.MAX_GRAND_PARENT_OVERLAP_BYTES;
import static java.util.Objects.requireNonNull;

public class Compaction
{
    private final Version inputVersion;
    private final int level;

    // 每次压缩都要从 "level" 、 "level+1"甚至 "level+2"
    private final List<FileMetaData> levelInputs;
    private final List<FileMetaData> levelUpInputs;
    private final List<FileMetaData> grandparents;
    private final List<FileMetaData>[] inputs;

    private final long maxOutputFileSize;
    private final VersionEdit edit = new VersionEdit();

    // 判读和level1、level2重叠的数据的状态变量
    // (parent == level_ + 1, grandparent == level_ + 2)

    // Index in grandparent_starts_
    private int grandparentIndex;

    // Some output key has been seen
    private boolean seenKey;

    // Bytes of overlap between current output and grandparent files
    private long overlappedBytes;

    // State for implementing IsBaseLevelForKey

    // levelPointers holds indices into inputVersion -> levels: our state
    // is that we are positioned at one of the file ranges for each
    // higher level than the ones involved in this compaction (i.e. for
    // all L >= level_ + 2).
    private final int[] levelPointers = new int[NUM_LEVELS];

    public Compaction(Version inputVersion, int level, List<FileMetaData> levelInputs, List<FileMetaData> levelUpInputs, List<FileMetaData> grandparents)
    {
        this.inputVersion = inputVersion;
        this.level = level;
        this.levelInputs = levelInputs;
        this.levelUpInputs = levelUpInputs;
        this.grandparents = ImmutableList.copyOf(requireNonNull(grandparents, "grandparents is null"));
        this.maxOutputFileSize = VersionSet.maxFileSizeForLevel(level);
        this.inputs = new List[] {levelInputs, levelUpInputs};
    }

    public int getLevel()
    {
        return level;
    }

    public List<FileMetaData> getLevelInputs()
    {
        return levelInputs;
    }

    public List<FileMetaData> getLevelUpInputs()
    {
        return levelUpInputs;
    }

    public VersionEdit getEdit()
    {
        return edit;
    }

    // 返回第which层的第i个文件
    public FileMetaData input(int which, int i)
    {
        checkArgument(which == 0 || which == 1, "which must be either 0 or 1");
        if (which == 0) {
            return levelInputs.get(i);
        }
        else {
            return levelUpInputs.get(i);
        }
    }

    // 返回压缩过程个最大的文件size
    public long getMaxOutputFileSize()
    {
        return maxOutputFileSize;
    }

    /**
     * 假设0层文件完成合并之后，1层文件同时达到了数据上限，同时需要进行合并。
     * 更加糟糕的是，在最差的情况下，0-n层的文件同时达到了合并的条件，每一层都需要进行合并。
     *
     * @return 是否要上移levelInputs
     */
    public boolean isTrivialMove()
    {
        /**
         * levelInputs层的文件个数只有一个；
         * 1. levelInputs层文件与levelInputs+1层文件没有重叠；
         * 2. levelInputs层文件与levelInputs+2层的文件重叠部分不超过10个文件；
         * 3. 当满足这几个条件时，可以将levelInputs层的该文件直接移至slevelInputs+1层
         */
        return (levelInputs.size() == 1 &&
                levelUpInputs.isEmpty() &&
                totalFileSize(grandparents) <= MAX_GRAND_PARENT_OVERLAP_BYTES);

    }
    // 返回参数文件第总size
    public static long totalFileSize(List<FileMetaData> files)
    {
        long sum = 0;
        for (FileMetaData file : files) {
            sum += file.getFileSize();
        }
        return sum;
    }

    // 将所有合并过的文件都在version中记录为删除
    public void addInputDeletions(VersionEdit edit)
    {
        for (FileMetaData input : levelInputs) {
            edit.deleteFile(level, input.getNumber());
        }
        for (FileMetaData input : levelUpInputs) {
            edit.deleteFile(level + 1, input.getNumber());
        }
    }

    // Returns true if the information we have available guarantees that
    // the compaction is producing data in "level+1" for which no data exists
    // in levels greater than "level+1".
    public boolean isBaseLevelForKey(Slice userKey)
    {
        UserComparator userComparator = inputVersion.getInternalKeyComparator().getUserComparator();
        for (int level = this.level + 2; level < NUM_LEVELS; level++) {
            List<FileMetaData> files = inputVersion.getFiles(level);
            while (levelPointers[level] < files.size()) {
                FileMetaData f = files.get(levelPointers[level]);
                if (userComparator.compare(userKey, f.getLargest().getUserKey()) <= 0) {
                    // We've advanced far enough
                    if (userComparator.compare(userKey, f.getSmallest().getUserKey()) >= 0) {
                        // Key falls in this file's range, so definitely not base level
                        return false;
                    }
                    break;
                }
                levelPointers[level]++;
            }
        }
        return true;
    }

    // Returns true iff we should stop building the current output
    // before processing "internal_key".
    public boolean shouldStopBefore(InternalKey internalKey)
    {
        // Scan to find earliest grandparent file that contains key.
        InternalKeyComparator internalKeyComparator = inputVersion.getInternalKeyComparator();
        while (grandparentIndex < grandparents.size() && internalKeyComparator.compare(internalKey, grandparents.get(grandparentIndex).getLargest()) > 0) {
            if (seenKey) {
                overlappedBytes += grandparents.get(grandparentIndex).getFileSize();
            }
            grandparentIndex++;
        }
        seenKey = true;

        if (overlappedBytes > MAX_GRAND_PARENT_OVERLAP_BYTES) {
            // Too much overlap for current output; start new output
            overlappedBytes = 0;
            return true;
        }
        else {
            return false;
        }
    }

    public List<FileMetaData>[] getInputs()
    {
        return inputs;
    }
}