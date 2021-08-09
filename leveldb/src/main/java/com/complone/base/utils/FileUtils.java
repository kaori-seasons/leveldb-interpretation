package com.complone.base.utils;

import com.google.common.collect.ImmutableList;
import com.google.common.io.Files;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public final class FileUtils
{
    private static final int TEMP_DIR_ATTEMPTS = 10000;

    private FileUtils()
    {
    }

    // 判断文件是否问软链接，软连接的定义：https://www.runoob.com/linux/linux-comm-ln.html
    // linux中的软链接类似于windows中的快捷方式，一个形象的例子：https://blog.csdn.net/weixin_44153121/article/details/85258047
    public static boolean isSymbolicLink(File file)
    {
        try {
            File canonicalFile = file.getCanonicalFile();
            File absoluteFile = file.getAbsoluteFile();
            File parentFile = file.getParentFile();
            return !canonicalFile.getName().equals(absoluteFile.getName()) ||
                    parentFile != null && !parentFile.getCanonicalPath().equals(canonicalFile.getParent());
        }
        catch (IOException e) {
            // error on the side of caution
            return true;
        }
    }

    public static ImmutableList<File> listFiles(File dir)
    {
        /**
         * list()方法是返回某个目录下的所有文件和目录的文件名，返回的是String数组
         *
         * listFiles()方法是返回某个目录下所有文件和目录的绝对路径，返回的是File数组
         *
         * 所谓的 immutable 对象是指对象的状态不可变，不可修改，因此这样的对象天生就具有线程安全性。
         * 由于 immutable 集合在创建时，就确定了元素的所有信息，不需要考虑后续的扩展问题
         */
        File[] files = dir.listFiles();
        if (files == null) {
            return ImmutableList.of();
        }
        return ImmutableList.copyOf(files);
    }

    public static ImmutableList<File> listFiles(File dir, FilenameFilter filter)
    {
        File[] files = dir.listFiles(filter);
        if (files == null) {
            return ImmutableList.of();
        }
        return ImmutableList.copyOf(files);
    }

    // 创建临时目录
    public static File createTempDir(String prefix)
    {
        // System.getproperty(“java.io.tmpdir”)是获取操作系统缓存的临时目录，不同操作系统的缓存临时目录不一样，
        //
        //   在Windows的缓存目录为：C:\Users\登录用户~1\AppData\Local\Temp\
        //
        //   Linux：/tmp
        return createTempDir(new File(System.getProperty("java.io.tmpdir")), prefix);
    }

    public static File createTempDir(File parentDir, String prefix)
    {
        String baseName = "";
        if (prefix != null) {
            baseName += prefix + "-";
        }

        baseName += System.currentTimeMillis() + "-";
        // 多次尝试，直到成功，一种乐观的解决方案
        for (int counter = 0; counter < TEMP_DIR_ATTEMPTS; counter++) {
            File tempDir = new File(parentDir, baseName + counter);
            if (tempDir.mkdir()) {
                return tempDir;
            }
        }
        throw new IllegalStateException("Failed to create directory within "
                + TEMP_DIR_ATTEMPTS + " attempts (tried "
                + baseName + "0 to " + baseName + (TEMP_DIR_ATTEMPTS - 1) + ')');
    }

    public static boolean deleteDirectoryContents(File directory)
    {
        checkArgument(directory.isDirectory(), "Not a directory: %s", directory);

        // 不需要删除软链接目录下的内容
        if (isSymbolicLink(directory)) {
            return false;
        }

        boolean success = true;
        for (File file : listFiles(directory)) {
            // 递归删除，设置标识为success
            success = deleteRecursively(file) && success;
        }
        return success;
    }

    public static boolean deleteRecursively(File file)
    {
        boolean success = true;
        if (file.isDirectory()) {
            success = deleteDirectoryContents(file);
        }

        return file.delete() && success;
    }

    public static boolean copyDirectoryContents(File src, File target)
    {
        checkArgument(src.isDirectory(), "Source dir is not a directory: %s", src);
        // 不需要复制软链接的内容
        if (isSymbolicLink(src)) {
            return false;
        }

        target.mkdirs();
        checkArgument(target.isDirectory(), "Target dir is not a directory: %s", src);

        boolean success = true;
        for (File file : listFiles(src)) {
            // 递归复制
            success = copyRecursively(file, new File(target, file.getName())) && success;
        }
        return success;
    }

    public static boolean copyRecursively(File src, File target)
    {
        if (src.isDirectory()) {
            return copyDirectoryContents(src, target);
        }
        else {
            try {
                Files.copy(src, target);
                return true;
            }
            catch (IOException e) {
                return false;
            }
        }
    }

    // 创建文件
    public static File newFile(String parent, String... paths)
    {
        requireNonNull(parent, "parent is null");
        requireNonNull(paths, "paths is null");

        return newFile(new File(parent), ImmutableList.copyOf(paths));
    }

    public static File newFile(File parent, String... paths)
    {
        requireNonNull(parent, "parent is null");
        requireNonNull(paths, "paths is null");

        return newFile(parent, ImmutableList.copyOf(paths));
    }

    public static File newFile(File parent, Iterable<String> paths)
    {
        requireNonNull(parent, "parent is null");
        requireNonNull(paths, "paths is null");

        File result = parent;
        for (String path : paths) {
            result = new File(result, path);
        }
        return result;
    }
}