package com.complone.base;

import java.io.Closeable;
import java.util.Map;

public interface DB
        extends Iterable<Map.Entry<byte[], byte[]>>, Closeable
{
    byte[] get(byte[] key)
            throws DBException;

    byte[] get(byte[] key, ReadOptions options)
            throws DBException;

    @Override
    DBIterator iterator();

    DBIterator iterator(ReadOptions options);

    void put(byte[] key, byte[] value)
            throws DBException;

    void delete(byte[] key)
            throws DBException;

    void write(WriteBatch updates)
            throws DBException;

    WriteBatch createWriteBatch();

    /**
     * @return null if options.isSnapshot()==false otherwise returns a snapshot
     * of the DB after this operation.
     */
    Snapshot put(byte[] key, byte[] value, WriteOptions options)
            throws DBException;

    /**
     * @return null if options.isSnapshot()==false otherwise returns a snapshot
     * of the DB after this operation.
     */
    Snapshot delete(byte[] key, WriteOptions options)
            throws DBException;

    /**
     * @return null if options.isSnapshot()==false otherwise returns a snapshot
     * of the DB after this operation.
     */
    Snapshot write(WriteBatch updates, WriteOptions options)
            throws DBException;

    Snapshot getSnapshot();

    long[] getApproximateSizes(Range... ranges);

    String getProperty(String name);

    /**
     * Suspends any background compaction threads.  This methods
     * returns once the background compactions are suspended.
     */
    void suspendCompactions()
            throws InterruptedException;

    /**
     * Resumes the background compaction threads.
     */
    void resumeCompactions();

    /**
     * Force a compaction of the specified key range.
     *
     * @param begin if null then compaction start from the first key
     * @param end if null then compaction ends at the last key
     */
    void compactRange(byte[] begin, byte[] end)
            throws DBException;
}
