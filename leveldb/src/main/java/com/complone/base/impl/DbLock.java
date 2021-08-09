package com.complone.base.impl;

import com.google.common.base.Throwables;
import com.complone.base.utils.Closeables;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class DbLock
{
    private final File lockFile;
    private final FileChannel channel;
    /**
     * 文件锁是进程级别的，不是线程级别的。文件锁可以解决多个进程并发访问、修改同一个文件的问题，但不能解决多线程并发访问、修改同一文件的问题。
     * 就是说使用文件锁时，同一进程内（同一个程序中）的多个线程，可以同时访问、修改此文件。
     *
     * 文件锁分为2类：
     * 1. 排它锁：又叫独占锁。对文件加排它锁后，该进程可以对此文件进行读写，该进程独占此文件，其他进程不能读写此文件，直到该进程释放文件锁。
     * 2. 共享锁：某个进程对文件加共享锁，其他进程也可以访问此文件，但这些进程都只能读此文件，不能写。线程是安全的。只要还有一个进程持有共享锁，此文件就只能读，不能写。
     *
     * 有4种获取文件锁的方法：
     *
     * 1. lock()    //对整个文件加锁，默认为排它锁。
     * 自定义加锁方式。前2个参数指定要加锁的部分（可以只对此文件的部分内容加锁），第三个参数值指定是否是共享锁。
     * 2. lock(long position, long size, boolean shared)
     * 3. tryLock()    //对整个文件加锁，默认为排它锁。
     * 4. tryLock(long position, long size, boolean  shared)     //自定义加锁方式。
     *
     * 如果指定为共享锁，则其它进程可读此文件，所有进程均不能写此文件，如果某进程试图对此文件进行写操作，会抛出异常。
     *
     * lock与tryLock的区别：
     *
     * lock是阻塞式的，如果未获取到文件锁，会一直阻塞当前线程，直到获取文件锁
     * tryLock和lock的作用相同，只不过tryLock是非阻塞式的，tryLock是尝试获取文件锁，获取成功就返回锁对象，否则返回null，不会阻塞当前线程。
     */
    private final FileLock lock;

    public DbLock(File lockFile)
            throws IOException
    {
        requireNonNull(lockFile, "lockFile is null");
        this.lockFile = lockFile;

        /**
         * 随机流（RandomAccessFile）不属于IO流，支持对文件的读取和写入随机访问。
         * 把随机访问的文件对象看作存储在文件系统中的一个大型 byte 数组，
         * 然后通过指向该 byte 数组的光标或索引（即：文件指针 FilePointer）在该数组任意位置读取或写入任意数据。
         */
        channel = new RandomAccessFile(lockFile, "rw").getChannel();
        try {
            // 非阻塞式的文件锁
            lock = channel.tryLock();
        }
        catch (IOException e) {
            Closeables.closeQuietly(channel);
            throw e;
        }

        if (lock == null) {
            throw new IOException(format("Unable to acquire lock on '%s'", lockFile.getAbsolutePath()));
        }
    }

    // 此文件锁是否还有效
    public boolean isValid()
    {
        return lock.isValid();
    }

    // 释放锁，关闭文件流
    public void release()
    {
        try {
            lock.release();
        }
        catch (IOException e) {
            Throwables.propagate(e);
        }
        finally {
            Closeables.closeQuietly(channel);
        }
    }

    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder();
        sb.append("DbLock");
        sb.append("{lockFile=").append(lockFile);
        sb.append(", lock=").append(lock);
        sb.append('}');
        return sb.toString();
    }
}
