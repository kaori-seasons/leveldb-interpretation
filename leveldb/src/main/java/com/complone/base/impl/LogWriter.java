package com.complone.base.impl;

import com.complone.base.include.Slice;

import java.io.File;
import java.io.IOException;

/**
 * 将log写入到file或者mmaptable中
 */
public interface LogWriter
{
    boolean isClosed();

    void close()
            throws IOException;

    void delete()
            throws IOException;

    File getFile();

    long getFileNumber();

    void addRecord(Slice record, boolean force)
            throws IOException;
}
