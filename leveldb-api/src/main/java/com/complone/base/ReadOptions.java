package com.complone.base;
public class ReadOptions
{
    private boolean verifyChecksums;
    private boolean fillCache = true;
    private Snapshot snapshot;

    public Snapshot snapshot()
    {
        return snapshot;
    }

    public ReadOptions snapshot(Snapshot snapshot)
    {
        this.snapshot = snapshot;
        return this;
    }

    public boolean fillCache()
    {
        return fillCache;
    }

    public ReadOptions fillCache(boolean fillCache)
    {
        this.fillCache = fillCache;
        return this;
    }

    public boolean verifyChecksums()
    {
        return verifyChecksums;
    }

    public ReadOptions verifyChecksums(boolean verifyChecksums)
    {
        this.verifyChecksums = verifyChecksums;
        return this;
    }
}
