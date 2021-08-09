package com.complone.base.impl;

import com.google.common.collect.ImmutableList;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;

/**
 * leveldb包含6种文件：
 * 1. <dbname>/[0-9]+.log：db操作日志
 * 2. <dbname>/[0-9]+.sst：db的sstable文件
 * 3. <dbname>/MANIFEST-[0-9]+：DB元信息文件
 * 4. <dbname>/CURRENT：记录当前正在使用的Manifest文件
 *  它的内容就是当前的manifest文件名；因为在LevleDb的运行过程中，随着Compaction的进行，新的SSTable文件被产生，老的文件被废弃。
 *  并生成新的Manifest文件来记载sstable的变动，而CURRENT则用来记录我们关心的Manifest文件。
 *  当db被重新打开时，leveldb总是生产一个新的manifest文件。Manifest文件使用log的格式，对服务状态的改变（新加或删除的文件）都会追加到该log中。
 * 5. <dbname>/log：系统的运行日志，记录系统的运行信息或者错误日志。
 * 6. <dbname>/dbtmp：临时数据库文件，repair时临时生成的。
 */
public final class Filename
{
    private Filename()
    {
    }

    public enum FileType
    {
        LOG,
        DB_LOCK,
        TABLE,
        DESCRIPTOR,
        CURRENT,
        TEMP,
        INFO_LOG  // Either the current one, or an old one
    }

    /**
     * 返回log文件名
     */
    public static String logFileName(long number)
    {
        return makeFileName(number, "log");
    }

    /**
     * 返回sstable文件名
     */
    public static String tableFileName(long number)
    {
        return makeFileName(number, "sst");
    }

    /**
     * 它记录的是leveldb的元信息，比如DB使用的Comparator名，以及各SSTable文件的管理信息：如Level层数、文件名、最小key和最大key等等。
     */
    public static String descriptorFileName(long number)
    {
        checkArgument(number >= 0, "number is negative");
        return String.format("MANIFEST-%06d", number);
    }

    /**
     * 返回当前文件的名称
     */
    public static String currentFileName()
    {
        return "CURRENT";
    }

    /**
     * Return the name of the lock file.
     */
    public static String lockFileName()
    {
        return "LOCK";
    }

    /**
     * 返回临时文件名称[0-9].dbtmp
     */
    public static String tempFileName(long number)
    {
        return makeFileName(number, "dbtmp");
    }

    /**
     * Return the name of the info log file.
     */
    public static String infoLogFileName()
    {
        return "LOG";
    }

    /**
     * Return the name of the old info log file.
     */
    public static String oldInfoLogFileName()
    {
        return "LOG.old";
    }

    /**
     * 将文件解析为FileInfo格式
     *
     * leveldb的文件名都有固定格式的，根据文件获得FileInfo,FileInfo包含文件类型和fileNumber
     */
    public static FileInfo parseFileName(File file)
    {
        // Owned filenames have the form:
        //    dbname/CURRENT
        //    dbname/LOCK
        //    dbname/LOG
        //    dbname/LOG.old
        //    dbname/MANIFEST-[0-9]+
        //    dbname/[0-9]+.(log|sst|dbtmp)
        String fileName = file.getName();
        if ("CURRENT".equals(fileName)) {
            return new FileInfo(FileType.CURRENT);
        }
        else if ("LOCK".equals(fileName)) {
            return new FileInfo(FileType.DB_LOCK);
        }
        else if ("LOG".equals(fileName)) {
            return new FileInfo(FileType.INFO_LOG);
        }
        else if ("LOG.old".equals(fileName)) {
            return new FileInfo(FileType.INFO_LOG);
        }
        else if (fileName.startsWith("MANIFEST-")) {
            long fileNumber = Long.parseLong(removePrefix(fileName, "MANIFEST-"));
            return new FileInfo(FileType.DESCRIPTOR, fileNumber);
        }
        else if (fileName.endsWith(".log")) {
            long fileNumber = Long.parseLong(removeSuffix(fileName, ".log"));
            return new FileInfo(FileType.LOG, fileNumber);
        }
        else if (fileName.endsWith(".sst")) {
            long fileNumber = Long.parseLong(removeSuffix(fileName, ".sst"));
            return new FileInfo(FileType.TABLE, fileNumber);
        }
        else if (fileName.endsWith(".dbtmp")) {
            long fileNumber = Long.parseLong(removeSuffix(fileName, ".dbtmp"));
            return new FileInfo(FileType.TEMP, fileNumber);
        }
        return null;
    }

    /**
     * 使得CURRENT指向descriptorNumber代表的文件
     *
     * @return true if successful; false otherwise
     */
    public static boolean setCurrentFile(File databaseDir, long descriptorNumber)
            throws IOException
    {
        // manifest为依据descriptorNumber生成的文件名:MANIFEST-[0-9]
        String manifest = descriptorFileName(descriptorNumber);
        // temp:[0-9].dbtmp
        String temp = tempFileName(descriptorNumber);
        // 创建临时文件
        File tempFile = new File(databaseDir, temp);
        // 将MANIFEST-[0-9]写入文件名为：temp的文件
        writeStringToFileSync(manifest + "\n", tempFile);

        File to = new File(databaseDir, currentFileName());
        // 替换CURRENT文件内容
        boolean ok = tempFile.renameTo(to);
        if (!ok) {
            // 如果没有替换成功，则删除掉新创建的文件
            tempFile.delete();
            writeStringToFileSync(manifest + "\n", to);
        }
        return ok;
    }

    private static void writeStringToFileSync(String str, File file)
            throws IOException
    {
        try (FileOutputStream stream = new FileOutputStream(file)) {
            stream.write(str.getBytes(UTF_8));
            stream.flush();
            stream.getFD().sync();
        }
    }

    public static List<File> listFiles(File dir)
    {
        // listFiles()是获取该目录下所有文件和目录的绝对路径
        File[] files = dir.listFiles();
        if (files == null) {
            return ImmutableList.of();
        }
        return ImmutableList.copyOf(files);
    }
    //按照number.suffix的格式返回文件名
    private static String makeFileName(long number, String suffix)
    {
        checkArgument(number >= 0, "number is negative");
        requireNonNull(suffix, "suffix is null");
        return String.format("%06d.%s", number, suffix);
    }

    private static String removePrefix(String value, String prefix)
    {
        return value.substring(prefix.length());
    }

    private static String removeSuffix(String value, String suffix)
    {
        return value.substring(0, value.length() - suffix.length());
    }

    public static class FileInfo
    {
        private final FileType fileType;
        private final long fileNumber;

        public FileInfo(FileType fileType)
        {
            this(fileType, 0);
        }

        public FileInfo(FileType fileType, long fileNumber)
        {
            requireNonNull(fileType, "fileType is null");
            this.fileType = fileType;
            this.fileNumber = fileNumber;
        }

        public FileType getFileType()
        {
            return fileType;
        }

        public long getFileNumber()
        {
            return fileNumber;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            FileInfo fileInfo = (FileInfo) o;

            if (fileNumber != fileInfo.fileNumber) {
                return false;
            }
            if (fileType != fileInfo.fileType) {
                return false;
            }

            return true;
        }

        @Override
        public int hashCode()
        {
            int result = fileType.hashCode();
            result = 31 * result + (int) (fileNumber ^ (fileNumber >>> 32));
            return result;
        }

        @Override
        public String toString()
        {
            StringBuilder sb = new StringBuilder();
            sb.append("FileInfo");
            sb.append("{fileType=").append(fileType);
            sb.append(", fileNumber=").append(fileNumber);
            sb.append('}');
            return sb.toString();
        }
    }
}
