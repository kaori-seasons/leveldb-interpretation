package com.complone.base;

public class WriteOptions
{
    private boolean sync;
    private boolean snapshot;

    public boolean sync()
    {
        return sync;
    }

    public WriteOptions sync(boolean sync)
    {
        this.sync = sync;
        return this;
    }

    public boolean snapshot()
    {
        return snapshot;
    }

    public WriteOptions snapshot(boolean snapshot)
    {
        this.snapshot = snapshot;
        return this;
    }
}
