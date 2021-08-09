package com.complone.base.impl;

/**
 * SST采取分层结构
 * 在读操作中，要查找一条entry，先查找log，如果没有找到，然后在Level 0中查找，如果还是没有找到，再依次查找Level 1、Level 2...
 * seekFileLevel记录sst层位
 * seekFile记录sst文件信息
 */
public class ReadStats
{
    private int seekFileLevel = -1;
    private FileMetaData seekFile;

    public void clear()
    {
        seekFileLevel = -1;
        seekFile = null;
    }

    public int getSeekFileLevel()
    {
        return seekFileLevel;
    }

    public void setSeekFileLevel(int seekFileLevel)
    {
        this.seekFileLevel = seekFileLevel;
    }

    public FileMetaData getSeekFile()
    {
        return seekFile;
    }

    public void setSeekFile(FileMetaData seekFile)
    {
        this.seekFile = seekFile;
    }
}
