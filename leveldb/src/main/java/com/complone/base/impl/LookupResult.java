package com.complone.base.impl;

import com.complone.base.include.Slice;

import static java.util.Objects.requireNonNull;

/**
 * 查找结果，返回有效数据 或 删除数据
 */
public class LookupResult
{
    public static LookupResult ok(LookupKey key, Slice value)
    {
        return new LookupResult(key, value, false);
    }

    public static LookupResult deleted(LookupKey key)
    {
        return new LookupResult(key, null, true);
    }

    private final LookupKey key;
    private final Slice value;
    private final boolean deleted;

    private LookupResult(LookupKey key, Slice value, boolean deleted)
    {
        requireNonNull(key, "key is null");
        this.key = key;
        if (value != null) {
            this.value = value.slice();
        }
        else {
            this.value = null;
        }
        this.deleted = deleted;
    }

    public LookupKey getKey()
    {
        return key;
    }

    public Slice getValue()
    {
        if (value == null) {
            return null;
        }
        return value;
    }

    public boolean isDeleted()
    {
        return deleted;
    }
}
