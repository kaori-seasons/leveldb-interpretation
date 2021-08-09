package com.complone.base.impl;

import com.complone.base.include.Slice;
import com.complone.base.table.UserComparator;

import static com.google.common.base.Preconditions.checkState;
import static com.complone.base.impl.SequenceNumber.MAX_SEQUENCE_NUMBER;

public class InternalUserComparator
        implements UserComparator
{
    private final InternalKeyComparator internalKeyComparator;

    public InternalUserComparator(InternalKeyComparator internalKeyComparator)
    {
        this.internalKeyComparator = internalKeyComparator;
    }

    @Override
    public int compare(Slice left, Slice right)
    {
        return internalKeyComparator.compare(new InternalKey(left), new InternalKey(right));
    }

    @Override
    public String name()
    {
        return internalKeyComparator.name();
    }

    @Override
    public Slice findShortestSeparator(
            Slice start,
            Slice limit)
    {
        // Attempt to shorten the user portion of the key
        Slice startUserKey = new InternalKey(start).getUserKey();
        Slice limitUserKey = new InternalKey(limit).getUserKey();

        Slice shortestSeparator = internalKeyComparator.getUserComparator().findShortestSeparator(startUserKey, limitUserKey);

        if (internalKeyComparator.getUserComparator().compare(startUserKey, shortestSeparator) < 0) {
            // User key has become larger.  Tack on the earliest possible
            // number to the shortened user key.
            InternalKey newInternalKey = new InternalKey(shortestSeparator, MAX_SEQUENCE_NUMBER, ValueType.VALUE);
            checkState(compare(start, newInternalKey.encode()) < 0); // todo
            checkState(compare(newInternalKey.encode(), limit) < 0); // todo

            return newInternalKey.encode();
        }

        return start;
    }

    @Override
    public Slice findShortSuccessor(Slice key)
    {
        Slice userKey = new InternalKey(key).getUserKey();
        Slice shortSuccessor = internalKeyComparator.getUserComparator().findShortSuccessor(userKey);

        if (internalKeyComparator.getUserComparator().compare(userKey, shortSuccessor) < 0) {
            // User key has become larger.  Tack on the earliest possible
            // number to the shortened user key.
            InternalKey newInternalKey = new InternalKey(shortSuccessor, MAX_SEQUENCE_NUMBER, ValueType.VALUE);
            checkState(compare(key, newInternalKey.encode()) < 0); // todo

            return newInternalKey.encode();
        }

        return key;
    }
}
