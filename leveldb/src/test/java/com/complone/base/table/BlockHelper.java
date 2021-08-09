package com.complone.base.table;

import com.complone.base.db.Slices;
import com.complone.base.impl.SeekingIterator;
import com.complone.base.include.Slice;
import com.complone.base.utils.DataUnit;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

import org.testng.Assert;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public final class BlockHelper
{
    private BlockHelper()
    {
    }

    public static int estimateBlockSize(int blockRestartInterval, List<BlockEntry> entries)
    {
        if (entries.isEmpty()) {
            return DataUnit.INT_UNIT;
        }
        int restartCount = (int) Math.ceil(1.0 * entries.size() / blockRestartInterval);
        return estimateEntriesSize(blockRestartInterval, entries) +
                (restartCount * DataUnit.INT_UNIT) +
                DataUnit.INT_UNIT;
    }

    @SafeVarargs
    public static <K, V> void assertSequence(SeekingIterator<K, V> seekingIterator, Map.Entry<K, V>... entries)
    {
        assertSequence(seekingIterator, Arrays.asList(entries));
    }

    public static <K, V> void assertSequence(SeekingIterator<K, V> seekingIterator, Iterable<? extends Map.Entry<K, V>> entries)
    {
        Assert.assertNotNull(seekingIterator, "blockIterator is not null");

        for (Map.Entry<K, V> entry : entries) {
            assertTrue(seekingIterator.hasNext());
            assertEntryEquals(seekingIterator.peek(), entry);
            assertEntryEquals(seekingIterator.next(), entry);
        }
        assertFalse(seekingIterator.hasNext());

        try {
            seekingIterator.peek();
            fail("expected NoSuchElementException");
        }
        catch (NoSuchElementException expected) {
        }
        try {
            seekingIterator.next();
            fail("expected NoSuchElementException");
        }
        catch (NoSuchElementException expected) {
        }
    }

    public static <K, V> void assertEntryEquals(Map.Entry<K, V> actual, Map.Entry<K, V> expected)
    {
        if (actual.getKey() instanceof Slice) {
            assertSliceEquals((Slice) actual.getKey(), (Slice) expected.getKey());
            assertSliceEquals((Slice) actual.getValue(), (Slice) expected.getValue());
        }
        assertEquals(actual, expected);
    }

    public static void assertSliceEquals(Slice actual, Slice expected)
    {
        assertEquals(actual.toString(UTF_8), expected.toString(UTF_8));
    }

    public static String beforeString(Map.Entry<String, ?> expectedEntry)
    {
        String key = expectedEntry.getKey();
        int lastByte = key.charAt(key.length() - 1);
        return key.substring(0, key.length() - 1) + ((char) (lastByte - 1));
    }

    public static String afterString(Map.Entry<String, ?> expectedEntry)
    {
        String key = expectedEntry.getKey();
        int lastByte = key.charAt(key.length() - 1);
        return key.substring(0, key.length() - 1) + ((char) (lastByte + 1));
    }

    public static Slice before(Map.Entry<Slice, ?> expectedEntry)
    {
        Slice slice = expectedEntry.getKey().copySlice(0, expectedEntry.getKey().length());
        int lastByte = slice.length() - 1;
        slice.setByte(lastByte, slice.getUnsignedByte(lastByte) - 1);
        return slice;
    }

    public static Slice after(Map.Entry<Slice, ?> expectedEntry)
    {
        Slice slice = expectedEntry.getKey().copySlice(0, expectedEntry.getKey().length());
        int lastByte = slice.length() - 1;
        slice.setByte(lastByte, slice.getUnsignedByte(lastByte) + 1);
        return slice;
    }

    public static int estimateEntriesSize(int blockRestartInterval, List<BlockEntry> entries)
    {
        int size = 0;
        Slice previousKey = null;
        int restartBlockCount = 0;
        for (BlockEntry entry : entries) {
            int nonSharedBytes;
            if (restartBlockCount < blockRestartInterval) {
                nonSharedBytes = entry.getKey().length() - BlockBuilder.calculateSharedBytes(entry.getKey(), previousKey);
            }
            else {
                nonSharedBytes = entry.getKey().length();
                restartBlockCount = 0;
            }
            size += nonSharedBytes +
                    entry.getValue().length() +
                    (DataUnit.INT_UNIT * 3); // 3 int 是前面三个表示长度的int

            previousKey = entry.getKey();
            restartBlockCount++;

        }
        return size;
    }

    static BlockEntry createBlockEntry(String key, String value)
    {
        return new BlockEntry(Slices.copiedBuffer(key, UTF_8), Slices.copiedBuffer(value, UTF_8));
    }
}
