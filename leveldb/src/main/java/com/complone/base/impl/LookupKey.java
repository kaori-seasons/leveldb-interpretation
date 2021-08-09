package com.complone.base.impl;

import com.complone.base.include.Slice;

/**
 * Memtable的查询接口传入的是LookupKey，它也是由User Key和Sequence Number组合而成的，
 * 从其构造函数：LookupKey(const Slice& user_key, SequenceNumber s)中分析出LookupKey的格式为：
 * | User key (string) | sequence number (7 bytes) | value type (1 byte) |
 *
 * 这里的Size是user key长度+8，也就是整个字符串长度了；
 */
public class LookupKey
{
    private final InternalKey key;

    public LookupKey(Slice userKey, long sequenceNumber)
    {
        key = new InternalKey(userKey, sequenceNumber, ValueType.VALUE);
    }

    public InternalKey getInternalKey()
    {
        return key;
    }

    public Slice getUserKey()
    {
        return key.getUserKey();
    }

    @Override
    public String toString()
    {
        return key.toString();
    }
}