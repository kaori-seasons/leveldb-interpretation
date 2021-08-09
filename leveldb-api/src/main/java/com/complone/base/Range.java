package com.complone.base;

public class Range
{
    private final byte[] start;
    private final byte[] limit;

    public byte[] limit()
    {
        return limit;
    }

    public byte[] start()
    {
        return start;
    }

    public Range(byte[] start, byte[] limit)
    {
        Options.checkArgNotNull(start, "start");
        Options.checkArgNotNull(limit, "limit");
        this.limit = limit;
        this.start = start;
    }
}
