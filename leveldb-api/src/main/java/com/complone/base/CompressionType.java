package com.complone.base;
/**
 * leveldb有两种压缩方式，一种是 不压缩，另外一种是SNAPPY压缩，对于Snappy压缩，如果压缩率太低<12.5%，还是作为未压缩内容存储。
 */
public enum CompressionType {
    NONE(0x00),
    SNAPPY(0x01);
    public static CompressionType getCompressionTypeByPersistentId(int persistentId)
    {
        for (CompressionType compressionType : CompressionType.values()) {
            if (compressionType.persistentId == persistentId) {
                return compressionType;
            }
        }
        throw new IllegalArgumentException("Unknown persistentId " + persistentId);
    }

    private final int persistentId;

    CompressionType(int persistentId)
    {
        this.persistentId = persistentId;
    }

    public int persistentId()
    {
        return persistentId;
    }
}
