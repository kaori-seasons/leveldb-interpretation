package com.complone.base.impl;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * FileMetaData数据结构用来维护一个文件的元信息，包括文件大小，文件编号，最大最小值，引用计数等
 * 其中引用计数记录了被不同的Version引用的个数，保证被引用中的文件不会被删除。
 */
public class FileMetaData
{
    /**
     * 文件编号
     */
    private final long number;

    /**
     * 文件大小（单位：byte）
     */
    private final long fileSize;

    /**
     * table的最小InternalKey
     */
    private final InternalKey smallest;

    /**
     * table的最大InternalKey
     */
    private final InternalKey largest;

    /**
     * 初始化引用计数
     */
    private final AtomicInteger allowedSeeks = new AtomicInteger(1 << 30);

    public FileMetaData(long number, long fileSize, InternalKey smallest, InternalKey largest)
    {
        this.number = number;
        this.fileSize = fileSize;
        this.smallest = smallest;
        this.largest = largest;
    }

    public long getFileSize()
    {
        return fileSize;
    }

    public long getNumber()
    {
        return number;
    }

    public InternalKey getSmallest()
    {
        return smallest;
    }

    public InternalKey getLargest()
    {
        return largest;
    }

    public int getAllowedSeeks()
    {
        return allowedSeeks.get();
    }

    public void setAllowedSeeks(int allowedSeeks)
    {
        this.allowedSeeks.set(allowedSeeks);
    }

    public void decrementAllowedSeeks()
    {
        allowedSeeks.getAndDecrement();
    }

    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder();
        sb.append("FileMetaData");
        sb.append("{number=").append(number);
        sb.append(", fileSize=").append(fileSize);
        sb.append(", smallest=").append(smallest);
        sb.append(", largest=").append(largest);
        sb.append(", allowedSeeks=").append(allowedSeeks);
        sb.append('}');
        return sb.toString();
    }
}