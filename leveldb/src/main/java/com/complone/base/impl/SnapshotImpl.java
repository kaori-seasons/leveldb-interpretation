package com.complone.base.impl;

import com.complone.base.Snapshot;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 快照（snapshot）也是基于sequence number实现的，即每一个sequence number代表着数据库的一个版本
 */
public class SnapshotImpl
        implements Snapshot
{
    private final AtomicBoolean closed = new AtomicBoolean();
    private final Version version;
    private final long lastSequence;

    SnapshotImpl(Version version, long lastSequence)
    {
        this.version = version;
        this.lastSequence = lastSequence;
        this.version.retain();
    }

    @Override
    public void close()
    {

        if (closed.compareAndSet(false, true)) {
            this.version.release();
        }
    }

    public long getLastSequence()
    {
        return lastSequence;
    }

    public Version getVersion()
    {
        return version;
    }

    @Override
    public String toString()
    {
        return Long.toString(lastSequence);
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        SnapshotImpl snapshot = (SnapshotImpl) o;

        if (lastSequence != snapshot.lastSequence) {
            return false;
        }
        if (!version.equals(snapshot.version)) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode()
    {
        int result = version.hashCode();
        result = 31 * result + (int) (lastSequence ^ (lastSequence >>> 32));
        return result;
    }
}
