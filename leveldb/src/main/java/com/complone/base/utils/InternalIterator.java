package com.complone.base.utils;

import com.complone.base.impl.InternalKey;
import com.complone.base.impl.SeekingIterator;
import com.complone.base.include.Slice;


public interface InternalIterator
        extends SeekingIterator<InternalKey, Slice>
{
}
