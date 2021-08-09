package com.complone.base.impl;

import com.complone.base.utils.Closeables;
import com.google.common.cache.*;
import com.complone.base.include.Slice;
import com.complone.base.table.FileChannelTable;
import com.complone.base.table.MMapTable;
import com.complone.base.table.Table;
import com.complone.base.table.UserComparator;
import com.complone.base.utils.Finalizer;
import com.complone.base.utils.InternalTableIterator;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.util.concurrent.ExecutionException;

import static java.util.Objects.requireNonNull;

/**
 * TableCache缓存的是Table对象，每个DB一个
 * 它内部使用一个LRUCache缓存所有的table对象，实际上其内容是文件编号{file number, TableAndFile}。
 *
 */
public class TableCache
{
    private final LoadingCache<Long, TableAndFile> cache;
    private final Finalizer<Table> finalizer = new Finalizer<>(1);

    public TableCache(final File databaseDir, int tableCacheSize, final UserComparator userComparator, final boolean verifyChecksums)
    {
        requireNonNull(databaseDir, "databaseName is null");
        // 初始化本地缓存，为缓存设置最大存储数量，设置监听器
        cache = CacheBuilder.newBuilder()
                .maximumSize(tableCacheSize)
                .removalListener(new RemovalListener<Long, TableAndFile>()
                {
                    @Override
                    public void onRemoval(RemovalNotification<Long, TableAndFile> notification)
                    {
                        // 缓存项被移除时,将文件关闭
                        Table table = notification.getValue().getTable();
                        // table.closer()继承自Callable
                        // 创建线程池、创建table的虚引用
                        finalizer.addCleanup(table, table.closer());
                    }
                })
                .build(new CacheLoader<Long, TableAndFile>()
                {
                    // 当本地缓存命没有中时，调用load方法获取结果并将结果缓存
                    @Override
                    public TableAndFile load(Long fileNumber)
                            throws IOException
                    {
                        // 这里是添加元素的地方
                        // 说明table不在cache中，则根据file number和db name打开一个RadomAccessFile。
                        // Table文件格式为：<db name>.<filenumber(%6u)>.sst。
                        // 如果文件打开成功，则调用Table::Open读取sstable文件。
                        return new TableAndFile(databaseDir, fileNumber, userComparator, verifyChecksums);
                    }
                });
    }

    public InternalTableIterator newIterator(FileMetaData file)
    {
        return newIterator(file.getNumber());
    }
    // 函数NewIterator()，返回一个可以遍历Table对象的Iterator指针
    public InternalTableIterator newIterator(long number)
    {
        return new InternalTableIterator(getTable(number).iterator());
    }
    // 获得key在文件中的偏移
    public long getApproximateOffsetOf(FileMetaData file, Slice key)
    {
        return getTable(file.getNumber()).getApproximateOffsetOf(key);
    }
    // 从缓存中获取table
    private Table getTable(long number)
    {
        Table table;
        try {
            table = cache.get(number).getTable();
        }
        catch (ExecutionException e) {
            Throwable cause = e;
            if (e.getCause() != null) {
                cause = e.getCause();
            }
            throw new RuntimeException("Could not open table " + number, cause);
        }
        return table;
    }

    public void close()
    {
        // 清除所有缓存项
        cache.invalidateAll();
        finalizer.destroy();
    }

    // 清楚文件缓存
    public void evict(long number)
    {
        cache.invalidate(number);
    }

    private static final class TableAndFile
    {
        private final Table table;

        private TableAndFile(File databaseDir, long fileNumber, UserComparator userComparator, boolean verifyChecksums)
                throws IOException
        {
            // sstable文件名
            String tableFileName = Filename.tableFileName(fileNumber);
            // 创建文件引用
            File tableFile = new File(databaseDir, tableFileName);
            FileInputStream fis = null;
            try {
                fis = new FileInputStream(tableFile);
                FileChannel fileChannel = fis.getChannel();
                if (LevelDBFactory.USE_MMAP) {
                    table = new MMapTable(tableFile.getAbsolutePath(), fileChannel, userComparator, verifyChecksums);
                    // 能走到这里，说明不需要文件流
                    Closeables.closeQuietly(fis);
                }
                else {
                    table = new FileChannelTable(tableFile.getAbsolutePath(), fileChannel, userComparator, verifyChecksums);
                }
            }
            catch (IOException ioe) {
                Closeables.closeQuietly(fis);
                throw ioe;
            }
        }

        public Table getTable()
        {
            return table;
        }
    }
}
