package com.complone.base.impl;

import com.complone.base.DB;
import com.complone.base.DBFactory;
import com.complone.base.Options;
import com.complone.base.utils.FileUtils;

import java.io.*;

import static java.nio.charset.StandardCharsets.UTF_8;

public class LevelDBFactory
        implements DBFactory
{
    public static final int CPU_DATA_MODEL;

    static {
        boolean is64bit;
        if (System.getProperty("os.name").contains("Windows")) {
            is64bit = System.getenv("ProgramFiles(x86)") != null;
        }
        else {
            is64bit = System.getProperty("os.arch").contains("64");
        }
        CPU_DATA_MODEL = is64bit ? 64 : 32;
    }
    // 只在64位系统上使用MMAP，因为在32位系统上，当所有数据都被映射到内存中时，很容易耗尽虚拟地址空间。
    // 如果真的想使用MMAP，请使用-Dleveldb.mmap=true
    public static final boolean USE_MMAP = Boolean.parseBoolean(System.getProperty("leveldb.mmap", "" + (CPU_DATA_MODEL > 32)));
    // 版本
    public static final String VERSION;

    static {
        String v = "unknown";
        // LevelDBFactory.class.getResourceAsStream("/version.txt"): 从类路径下也就是从classes文件夹下查找资源
        // 即：....../target/classes/ 下面找version.txt
        // LevelDBFactory.class.getResourceAsStream("version.txt"): 从当前类所在的包下查找资源
        // 即：....../target/classes/com.complone.base/impl/ 下面找version.txt
        InputStream is = LevelDBFactory.class.getResourceAsStream("version.txt");
        try {
            // 读取文件版本
            v = new BufferedReader(new InputStreamReader(is, UTF_8)).readLine();
        }
        catch (Throwable e) {
        }
        finally {
            try {
                is.close();
            }
            catch (Throwable e) {
            }
        }
        VERSION = v;
    }

    public static final LevelDBFactory factory = new LevelDBFactory();

    @Override
    public DB open(File path, Options options)
            throws IOException
    {
        return new DbImpl(options, path);
    }

    @Override
    public void destroy(File path, Options options)
            throws IOException
    {
        // 递归删除文件
        FileUtils.deleteRecursively(path);
    }

    @Override
    public void repair(File path, Options options)
            throws IOException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public String toString()
    {
        return String.format("iq80 leveldb version %s", VERSION);
    }

    public static byte[] bytes(String value)
    {
        return (value == null) ? null : value.getBytes(UTF_8);
    }

    public static String asString(byte[] value)
    {
        return (value == null) ? null : new String(value, UTF_8);
    }
}
