package com.complone.base.impl;

import com.complone.base.include.SliceInput;
import com.complone.base.include.SliceOutput;
import com.complone.base.utils.VariableLengthQuantity;

import java.util.Map;

import static com.complone.base.db.Slices.readLengthPrefixedBytes;
import static com.complone.base.db.Slices.writeLengthPrefixedBytes;
import static java.nio.charset.StandardCharsets.UTF_8;

public enum VersionEditTag
{
    // 8 is no longer used. It was used for large value refs.

    COMPARATOR(1)
            {
                @Override
                public void readValue(SliceInput sliceInput, VersionEdit versionEdit)
                {
                    byte[] bytes = new byte[VariableLengthQuantity.readVariableLengthInt(sliceInput)];
                    sliceInput.readBytes(bytes);
                    versionEdit.setComparatorName(new String(bytes, UTF_8));
                }

                @Override
                public void writeValue(SliceOutput sliceOutput, VersionEdit versionEdit)
                {
                    String comparatorName = versionEdit.getComparatorName();
                    if (comparatorName != null) {
                        VariableLengthQuantity.writeVariableLengthInt(getPersistentId(), sliceOutput);
                        byte[] bytes = comparatorName.getBytes(UTF_8);
                        VariableLengthQuantity.writeVariableLengthInt(bytes.length, sliceOutput);
                        sliceOutput.writeBytes(bytes);
                    }
                }
            },
    LOG_NUMBER(2)
            {
                @Override
                public void readValue(SliceInput sliceInput, VersionEdit versionEdit)
                {
                    versionEdit.setLogNumber(VariableLengthQuantity.readVariableLengthLong(sliceInput));
                }

                @Override
                public void writeValue(SliceOutput sliceOutput, VersionEdit versionEdit)
                {
                    Long logNumber = versionEdit.getLogNumber();
                    if (logNumber != null) {
                        VariableLengthQuantity.writeVariableLengthInt(getPersistentId(), sliceOutput);
                        VariableLengthQuantity.writeVariableLengthLong(logNumber, sliceOutput);
                    }
                }
            },

    PREVIOUS_LOG_NUMBER(9)
            {
                @Override
                public void readValue(SliceInput sliceInput, VersionEdit versionEdit)
                {
                    long previousLogNumber = VariableLengthQuantity.readVariableLengthLong(sliceInput);
                    versionEdit.setPreviousLogNumber(previousLogNumber);
                }

                @Override
                public void writeValue(SliceOutput sliceOutput, VersionEdit versionEdit)
                {
                    Long previousLogNumber = versionEdit.getPreviousLogNumber();
                    if (previousLogNumber != null) {
                        VariableLengthQuantity.writeVariableLengthInt(getPersistentId(), sliceOutput);
                        VariableLengthQuantity.writeVariableLengthLong(previousLogNumber, sliceOutput);
                    }
                }
            },

    NEXT_FILE_NUMBER(3)
            {
                @Override
                public void readValue(SliceInput sliceInput, VersionEdit versionEdit)
                {
                    versionEdit.setNextFileNumber(VariableLengthQuantity.readVariableLengthLong(sliceInput));
                }

                @Override
                public void writeValue(SliceOutput sliceOutput, VersionEdit versionEdit)
                {
                    Long nextFileNumber = versionEdit.getNextFileNumber();
                    if (nextFileNumber != null) {
                        VariableLengthQuantity.writeVariableLengthInt(getPersistentId(), sliceOutput);
                        VariableLengthQuantity.writeVariableLengthLong(nextFileNumber, sliceOutput);
                    }
                }
            },

    LAST_SEQUENCE(4)
            {
                @Override
                public void readValue(SliceInput sliceInput, VersionEdit versionEdit)
                {
                    versionEdit.setLastSequenceNumber(VariableLengthQuantity.readVariableLengthLong(sliceInput));
                }

                @Override
                public void writeValue(SliceOutput sliceOutput, VersionEdit versionEdit)
                {
                    Long lastSequenceNumber = versionEdit.getLastSequenceNumber();
                    if (lastSequenceNumber != null) {
                        VariableLengthQuantity.writeVariableLengthInt(getPersistentId(), sliceOutput);
                        VariableLengthQuantity.writeVariableLengthLong(lastSequenceNumber, sliceOutput);
                    }
                }
            },

    COMPACT_POINTER(5)
            {
                @Override
                public void readValue(SliceInput sliceInput, VersionEdit versionEdit)
                {
                    // level
                    int level = VariableLengthQuantity.readVariableLengthInt(sliceInput);

                    // internal key
                    InternalKey internalKey = new InternalKey(readLengthPrefixedBytes(sliceInput));

                    versionEdit.setCompactPointer(level, internalKey);
                }

                @Override
                public void writeValue(SliceOutput sliceOutput, VersionEdit versionEdit)
                {
                    for (Map.Entry<Integer, InternalKey> entry : versionEdit.getCompactPointers().entrySet()) {
                        VariableLengthQuantity.writeVariableLengthInt(getPersistentId(), sliceOutput);

                        // level
                        VariableLengthQuantity.writeVariableLengthInt(entry.getKey(), sliceOutput);

                        // internal key
                        writeLengthPrefixedBytes(sliceOutput, entry.getValue().encode());
                    }
                }
            },

    DELETED_FILE(6)
            {
                @Override
                public void readValue(SliceInput sliceInput, VersionEdit versionEdit)
                {
                    // level
                    int level = VariableLengthQuantity.readVariableLengthInt(sliceInput);

                    // file number
                    long fileNumber = VariableLengthQuantity.readVariableLengthLong(sliceInput);

                    versionEdit.deleteFile(level, fileNumber);
                }

                @Override
                public void writeValue(SliceOutput sliceOutput, VersionEdit versionEdit)
                {
                    for (Map.Entry<Integer, Long> entry : versionEdit.getDeletedFiles().entries()) {
                        VariableLengthQuantity.writeVariableLengthInt(getPersistentId(), sliceOutput);

                        // level
                        VariableLengthQuantity.writeVariableLengthInt(entry.getKey(), sliceOutput);

                        // file number
                        VariableLengthQuantity.writeVariableLengthLong(entry.getValue(), sliceOutput);
                    }
                }
            },

    NEW_FILE(7)
            {
                @Override
                public void readValue(SliceInput sliceInput, VersionEdit versionEdit)
                {
                    // level
                    int level = VariableLengthQuantity.readVariableLengthInt(sliceInput);

                    // file number
                    long fileNumber = VariableLengthQuantity.readVariableLengthLong(sliceInput);

                    // file size
                    long fileSize = VariableLengthQuantity.readVariableLengthLong(sliceInput);

                    // smallest key
                    InternalKey smallestKey = new InternalKey(readLengthPrefixedBytes(sliceInput));

                    // largest key
                    InternalKey largestKey = new InternalKey(readLengthPrefixedBytes(sliceInput));

                    versionEdit.addFile(level, fileNumber, fileSize, smallestKey, largestKey);
                }

                @Override
                public void writeValue(SliceOutput sliceOutput, VersionEdit versionEdit)
                {
                    for (Map.Entry<Integer, FileMetaData> entry : versionEdit.getNewFiles().entries()) {
                        VariableLengthQuantity.writeVariableLengthInt(getPersistentId(), sliceOutput);

                        // level
                        VariableLengthQuantity.writeVariableLengthInt(entry.getKey(), sliceOutput);

                        // file number
                        FileMetaData fileMetaData = entry.getValue();
                        VariableLengthQuantity.writeVariableLengthLong(fileMetaData.getNumber(), sliceOutput);

                        // file size
                        VariableLengthQuantity.writeVariableLengthLong(fileMetaData.getFileSize(), sliceOutput);

                        // smallest key
                        writeLengthPrefixedBytes(sliceOutput, fileMetaData.getSmallest().encode());

                        // smallest key
                        writeLengthPrefixedBytes(sliceOutput, fileMetaData.getLargest().encode());
                    }
                }
            };

    public static VersionEditTag getValueTypeByPersistentId(int persistentId)
    {
        for (VersionEditTag compressionType : VersionEditTag.values()) {
            if (compressionType.persistentId == persistentId) {
                return compressionType;
            }
        }
        throw new IllegalArgumentException(String.format("Unknown %s persistentId %d", VersionEditTag.class.getSimpleName(), persistentId));
    }

    private final int persistentId;

    VersionEditTag(int persistentId)
    {
        this.persistentId = persistentId;
    }

    public int getPersistentId()
    {
        return persistentId;
    }

    public abstract void readValue(SliceInput sliceInput, VersionEdit versionEdit);

    public abstract void writeValue(SliceOutput sliceOutput, VersionEdit versionEdit);
}