/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.complone.base.impl;

import com.complone.base.include.Slice;
import com.complone.base.table.UserComparator;
import com.complone.base.utils.MergingIterator;
import com.google.common.base.Joiner;
import com.google.common.collect.*;
import com.google.common.io.Files;
import com.complone.base.utils.InternalIterator;
import com.complone.base.utils.Level0Iterator;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

import static com.complone.base.impl.DbConstants.NUM_LEVELS;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;

public class VersionSet
        implements SeekingIterable<InternalKey, Slice>{
    private static final int L0_COMPACTION_TRIGGER = 4;

    public static final int TARGET_FILE_SIZE = 2 * 1048576; // 2M

    // Maximum bytes of overlaps in grandparent (i.e., level+2) before we
    // stop building a single file in a level.level+1 compaction.
    public static final long MAX_GRAND_PARENT_OVERLAP_BYTES = 10 * TARGET_FILE_SIZE;
    // nextFileNumber从2开始
    private final AtomicLong nextFileNumber = new AtomicLong(2);
    /**
     * 一个Manifest文件中，包含了多条Session Record。一个Session Record记录了从上一个版本至该版本的变化情况。
     * 变化情况大致包括：
     * （1）新增了哪些sstable文件；
     * （2）删除了哪些sstable文件（由于compaction导致）；
     * （3）最新的journal日志文件标号等；
     * 借助这个Manifest文件，leveldb启动时，可以根据一个初始的版本状态，不断地应用这些版本改动，使得系统的版本信息恢复到最近一次使用的状态。
     * 一个Manifest内部包含若干条Session Record，其中第一条Session Record记载了当时leveldb的全量版本信息，
     * 其余若干条Session Record仅记录每次更迭的变化情况。
     */
    private long manifestFileNumber = 1;
    // 当前version
    private Version current;
    // 获取、设置last sequence，set时不能后退
    private long lastSequence;
    // 返回当前log文件编号
    private long logNumber;
    private long prevLogNumber;
    // activeVersions保存version信息，我们这里想要在用version的时候就可以拿到，但是又不希望影响key(也就是version)的生命周期，所以用了弱引用
    private final Map<Version, Object> activeVersions = new MapMaker().weakKeys().makeMap();
    private final File databaseDir;
    private final TableCache tableCache;
    private final InternalKeyComparator internalKeyComparator;

    private LogWriter descriptorLog;
    private final Map<Integer, InternalKey> compactPointers = new TreeMap<>();

    // VersionSet会使用到TableCache，这个是调用者传入的。TableCache用于Get k/v操作
    public VersionSet(File databaseDir, TableCache tableCache, InternalKeyComparator internalKeyComparator)
            throws IOException
    {
        this.databaseDir = databaseDir;
        this.tableCache = tableCache;
        this.internalKeyComparator = internalKeyComparator;
        // 创建新的Version并加入到Version链表中，并设置CURRENT=新创建version；
        appendVersion(new Version(this));

        initializeIfNeeded();
    }

    private void initializeIfNeeded()
            throws IOException
    {
        File currentFile = new File(databaseDir, Filename.currentFileName());

        if (!currentFile.exists()) {
            VersionEdit edit = new VersionEdit();
            edit.setComparatorName(internalKeyComparator.name());
            edit.setLogNumber(prevLogNumber);
            edit.setNextFileNumber(nextFileNumber.get());
            edit.setLastSequenceNumber(lastSequence);
            // 创建可以写的文件
            LogWriter log = Logs.createLogWriter(new File(databaseDir, Filename.descriptorFileName(manifestFileNumber)), manifestFileNumber);
            try {
                // 第一条包括leveldb全量信息
                writeSnapshot(log);
                // 这些记录了每次更迭的变化情况
                log.addRecord(edit.encode(), false);
            }
            finally {
                log.close();
            }

            Filename.setCurrentFile(databaseDir, log.getFileNumber());
        }
    }

    public void destroy()
            throws IOException
    {
        if (descriptorLog != null) {
            descriptorLog.close();
            descriptorLog = null;
        }

        Version t = current;
        if (t != null) {
            current = null;
            t.release();
        }

        Set<Version> versions = activeVersions.keySet();
    }

    /**
     * 把v加入到versionset中，并设置为current version。并释放老的current version
     */
    private void appendVersion(Version version)
    {
        requireNonNull(version, "version is null");
        checkArgument(version != current, "version is the current version");
        Version previous = current;
        current = version;
        activeVersions.put(version, new Object());
        if (previous != null) {
            previous.release();
        }
    }

    public void removeVersion(Version version)
    {
        requireNonNull(version, "version is null");
        checkArgument(version != current, "version is the current version");
        boolean removed = activeVersions.remove(version) != null;
        assert removed : "Expected the version to still be in the active set";
    }

    public InternalKeyComparator getInternalKeyComparator()
    {
        return internalKeyComparator;
    }

    public TableCache getTableCache()
    {
        return tableCache;
    }

    public Version getCurrent()
    {
        return current;
    }
    // 当前的MANIFEST文件号
    public long getManifestFileNumber()
    {
        return manifestFileNumber;
    }

    public long getNextFileNumber()
    {
        return nextFileNumber.getAndIncrement();
    }
    // 返回当前log文件编号
    public long getLogNumber()
    {
        return logNumber;
    }

    public long getPrevLogNumber()
    {
        return prevLogNumber;
    }

    @Override
    public MergingIterator iterator()
    {
        return current.iterator();
    }

    public MergingIterator makeInputIterator(Compaction c)
    {
        // Level-0 files have to be merged together.  For other levels,
        // we will make a concatenating iterator per level.
        // TODO(opt): use concatenating iterator for level-0 if there is no overlap
        List<InternalIterator> list = new ArrayList<>();
        for (int which = 0; which < 2; which++) {
            if (!c.getInputs()[which].isEmpty()) {
                if (c.getLevel() + which == 0) {
                    List<FileMetaData> files = c.getInputs()[which];
                    list.add(new Level0Iterator(tableCache, files, internalKeyComparator));
                }
                else {
                    // Create concatenating iterator for the files from this level
                    list.add(Level.createLevelConcatIterator(tableCache, c.getInputs()[which], internalKeyComparator));
                }
            }
        }
        return new MergingIterator(list, internalKeyComparator);
    }

    public LookupResult get(LookupKey key)
    {
        return current.get(key);
    }

    public boolean overlapInLevel(int level, Slice smallestUserKey, Slice largestUserKey)
    {
        return current.overlapInLevel(level, smallestUserKey, largestUserKey);
    }
    // 返回指定level的文件个数
    public int numberOfFilesInLevel(int level)
    {
        return current.numberOfFilesInLevel(level);
    }
    // 返回指定level中所有sstable文件大小的和
    public long numberOfBytesInLevel(int level)
    {
        return current.numberOfFilesInLevel(level);
    }

    public long getLastSequence()
    {
        return lastSequence;
    }

    // 获取、设置last sequence，set时不能后退
    public void setLastSequence(long newLastSequence)
    {
        checkArgument(newLastSequence >= lastSequence, "Expected newLastSequence to be greater than or equal to current lastSequence");
        this.lastSequence = newLastSequence;
    }

    // 在current version上应用指定的VersionEdit，生成新的MANIFEST信息，保存到磁盘上，并用作current version。
    public void logAndApply(VersionEdit edit)
            throws IOException
    {
        // 为edit设置log number等4个计数器。
        // 要保证edit自己的log number是比较大的那个，否则就是致命错误。保证edit的log number小于next file number，否则就是致命错误。
        if (edit.getLogNumber() != null) {
            checkArgument(edit.getLogNumber() >= logNumber);
            checkArgument(edit.getLogNumber() < nextFileNumber.get());
        }
        else {
            edit.setLogNumber(logNumber);
        }

        if (edit.getPreviousLogNumber() == null) {
            edit.setPreviousLogNumber(prevLogNumber);
        }

        edit.setNextFileNumber(nextFileNumber.get());
        edit.setLastSequenceNumber(lastSequence);
        // 创建一个新的Version v，并把新的edit变动保存到v中。
        Version version = new Version(this);
        Builder builder = new Builder(this, current);
        builder.apply(edit);
        builder.saveTo(version);
        // 如前分析，只是为v计算执行compaction的最佳level
        finalizeVersion(version);

        boolean createdNewManifest = false;
        try {
            // 如果MANIFEST文件指针不存在，就创建并初始化一个新的MANIFEST文件。
            // 这只会发生在第一次打开数据库时。这个MANIFEST文件保存了current version的快照。
            if (descriptorLog == null) {
                edit.setNextFileNumber(nextFileNumber.get());
                descriptorLog = Logs.createLogWriter(new File(databaseDir, Filename.descriptorFileName(manifestFileNumber)), manifestFileNumber);
                writeSnapshot(descriptorLog);// 写入快照
                createdNewManifest = true;
            }

            // 序列化current version信息
            Slice record = edit.encode();
            // append到MANIFEST log中
            descriptorLog.addRecord(record, true);

            //如果刚才创建了一个MANIFEST文件，通过写一个指向它的CURRENT文件
            //安装它；不需要再次检查MANIFEST是否出错，因为如果出错后面会删除它
            if (createdNewManifest) {
                Filename.setCurrentFile(databaseDir, descriptorLog.getFileNumber());
            }
        }
        catch (IOException e) {
            // Manifest创建失败
            if (createdNewManifest) {
                descriptorLog.close();
                new File(databaseDir, Filename.logFileName(descriptorLog.getFileNumber())).delete();
                descriptorLog = null;
            }
            throw e;
        }

        // 安装这个新的version
        appendVersion(version);
        logNumber = edit.getLogNumber();
        prevLogNumber = edit.getPreviousLogNumber();
    }

    /**
     * 通常用作写manifest文件的第一条记录，通常包括
     * 1. Comparator的名称；
     * 2. 最新的journal文件编号；
     * 3. 下一个可以使用的文件编号；
     * 4. 数据库已经持久化数据项中最大的sequence number；
     * 5. 新增的文件信息；
     * 6. 删除的文件信息；
     * 7. compaction记录信息；
     *
     * 或者用于把current version保存到log中，信息包括comparator名字、compaction点和各级sstable文件，函数逻辑很直观。
     *
     * @param log
     * @throws IOException
     */
    private void writeSnapshot(LogWriter log)
            throws IOException
    {
        // Save metadata
        VersionEdit edit = new VersionEdit();
        edit.setComparatorName(internalKeyComparator.name());

        // Save compaction pointers
        edit.setCompactPointers(compactPointers);

        // Save files
        edit.addFiles(current.getFiles());

        Slice record = edit.encode();
        log.addRecord(record, false);
    }
    // 恢复函数，从磁盘恢复最后保存的元信息
    public void recover()
            throws IOException
    {
        // 根据CURRENT指定的MANIFEST，读取db元信息。
        File currentFile = new File(databaseDir, Filename.currentFileName());
        checkState(currentFile.exists(), "CURRENT file does not exist");

        // 读取CURRENT文件中的内容，也就是最新的MANIFEST文件名，CURRENT文件以\n结尾，读取后需要trim下
        String currentName = Files.toString(currentFile, UTF_8);
        if (currentName.isEmpty() || currentName.charAt(currentName.length() - 1) != '\n') {
            throw new IllegalStateException("CURRENT file does not end with newline");
        }
        currentName = currentName.substring(0, currentName.length() - 1);

        // 根据最新的MANIFEST文件名打开MANIFEST文件
        // 数据流会在 try 执行完毕后自动被关闭
        try (FileInputStream fis = new FileInputStream(new File(databaseDir, currentName));
             FileChannel fileChannel = fis.getChannel()) {
            // read log edit log
            Long nextFileNumber = null;
            Long lastSequence = null;
            Long logNumber = null;
            Long prevLogNumber = null;
            // 构建current每一层的状态信息
            Builder builder = new Builder(this, current);

            LogReader reader = new LogReader(fileChannel, LogMonitors.throwExceptionMonitor(), true, 0);
            // 读取MANIFEST内容，MANIFEST是以log的方式写入的，因此这里调用的是log::Reader来读取。
            for (Slice record = reader.readRecord(); record != null; record = reader.readRecord()) {
                // 然后调用VersionEdit::DecodeFrom，从内容解析出VersionEdit对象
                VersionEdit edit = new VersionEdit(record);

                // verify comparator
                String editComparator = edit.getComparatorName();
                String userComparator = internalKeyComparator.name();
                checkArgument(editComparator == null || editComparator.equals(userComparator),
                        "Expected user comparator %s to match existing database comparator ", userComparator, editComparator);

                // 将VersionEdit记录的改动应用到versionSet中
                builder.apply(edit);

                // 读取MANIFEST中的log number, prev log number, nextfile number, last sequence。
                logNumber = coalesce(edit.getLogNumber(), logNumber);
                prevLogNumber = coalesce(edit.getPreviousLogNumber(), prevLogNumber);
                nextFileNumber = coalesce(edit.getNextFileNumber(), nextFileNumber);
                lastSequence = coalesce(edit.getLastSequenceNumber(), lastSequence);
            }

            List<String> problems = new ArrayList<>();
            if (nextFileNumber == null) {
                problems.add("Descriptor does not contain a meta-nextfile entry");
            }
            if (logNumber == null) {
                problems.add("Descriptor does not contain a meta-lognumber entry");
            }
            if (lastSequence == null) {
                problems.add("Descriptor does not contain a last-sequence-number entry");
            }
            if (!problems.isEmpty()) {
                throw new RuntimeException("Corruption: \n\t" + Joiner.on("\n\t").join(problems));
            }

            if (prevLogNumber == null) {
                prevLogNumber = 0L;
            }

            Version newVersion = new Version(this);
            // 把当前的状态合并到newVersion
            builder.saveTo(newVersion);

            // finalizeVersion(v)和AppendVersion(v)用来安装并使用version v
            finalizeVersion(newVersion);

            appendVersion(newVersion);
            manifestFileNumber = nextFileNumber;
            this.nextFileNumber.set(nextFileNumber + 1);
            this.lastSequence = lastSequence;
            this.logNumber = logNumber;
            this.prevLogNumber = prevLogNumber;
        }
    }

    /**
     * 该函数依照规则为下次的compaction计算出最适用的level，对于level 0和>0需要分别对待，逻辑如下。
     * @param version
     */
    private void finalizeVersion(Version version)
    {
        // Precomputed best level for next compaction
        int bestLevel = -1;
        double bestScore = -1;

        for (int level = 0; level < version.numberOfLevels() - 1; level++) {
            double score;
            if (level == 0) {
                // level0和其它level计算方法不同，原因如下，这也是leveldb为compaction所做的另一个优化。
                // 1. 对于较大的写缓存（write-buffer），做太多的level 0 compaction并不好
                // 2. 每次read操作都要merge level 0的所有文件，因此我们不希望level 0有太多的小文件存在
                // （比如写缓存太小，或者压缩比较高，或者覆盖/删除较多导致小文件太多）。这里的写缓存应该就是配置的操作log大小。
                // 对于level 0以文件个数计算，L0_COMPACTION_TRIGGER默认配置为4
                score = 1.0 * version.numberOfFilesInLevel(level) / L0_COMPACTION_TRIGGER;
            }
            else {
                // 对于level>0，根据level内的文件总大小计算
                long levelBytes = 0;
                for (FileMetaData fileMetaData : version.getFiles(level)) {
                    levelBytes += fileMetaData.getFileSize();
                }
                // maxBytesForLevel：根据level返回其本层文件总大小的预定最大值。
                score = 1.0 * levelBytes / maxBytesForLevel(level);
            }
            // 找到文件最大的level和score
            if (score > bestScore) {
                bestLevel = level;
                bestScore = score;
            }
        }

        version.setCompactionLevel(bestLevel);
        version.setCompactionScore(bestScore);
    }

    // 返回各参数表达式中第一个非空值
    private static <V> V coalesce(V... values)
    {
        for (V value : values) {
            if (value != null) {
                return value;
            }
        }
        return null;
    }
    // 获取函数，把所有version的所有level的文件加入到live中
    public List<FileMetaData> getLiveFiles()
    {
        ImmutableList.Builder<FileMetaData> builder = ImmutableList.builder();
        for (Version activeVersion : activeVersions.keySet()) {
            builder.addAll(activeVersion.getFiles().values());
        }
        return builder.build();
    }

    // maxBytesForLevel
    private static double maxBytesForLevel(int level)
    {
        // level 0 用不到这个计算规则，因为level 0 是基于文件数量的
        // 1048576 = 1024 * 1024，也就是1M
        double result = 10 * 1048576.0;  // Result for both level-0 and level-1
        while (level > 1) {
            result *= 10;
            level--;
        }
        return result;
    }

    public static long maxFileSizeForLevel(int level)
    {
        return TARGET_FILE_SIZE;  // We could vary per level to reduce number of files?
    }

    public boolean needsCompaction()
    {
        return current.getCompactionScore() >= 1 || current.getFileToCompact() != null;
    }

    public Compaction compactRange(int level, InternalKey begin, InternalKey end)
    {
        List<FileMetaData> levelInputs = getOverlappingInputs(level, begin, end);
        if (levelInputs.isEmpty()) {
            return null;
        }

        return setupOtherInputs(level, levelInputs);
    }

    public Compaction pickCompaction()
    {
        // We prefer compactions triggered by too much data in a level over
        // the compactions triggered by seeks.
        boolean sizeCompaction = (current.getCompactionScore() >= 1);
        boolean seekCompaction = (current.getFileToCompact() != null);

        int level;
        List<FileMetaData> levelInputs;
        if (sizeCompaction) {
            level = current.getCompactionLevel();
            checkState(level >= 0);
            checkState(level + 1 < NUM_LEVELS);

            // Pick the first file that comes after compact_pointer_[level]
            levelInputs = new ArrayList<>();
            for (FileMetaData fileMetaData : current.getFiles(level)) {
                if (!compactPointers.containsKey(level) ||
                        internalKeyComparator.compare(fileMetaData.getLargest(), compactPointers.get(level)) > 0) {
                    levelInputs.add(fileMetaData);
                    break;
                }
            }
            if (levelInputs.isEmpty()) {
                // Wrap-around to the beginning of the key space
                levelInputs.add(current.getFiles(level).get(0));
            }
        }
        else if (seekCompaction) {
            level = current.getFileToCompactLevel();
            levelInputs = ImmutableList.of(current.getFileToCompact());
        }
        else {
            return null;
        }

        // Files in level 0 may overlap each other, so pick up all overlapping ones
        if (level == 0) {
            Map.Entry<InternalKey, InternalKey> range = getRange(levelInputs);
            // Note that the next call will discard the file we placed in
            // c->inputs_[0] earlier and replace it with an overlapping set
            // which will include the picked file.
            levelInputs = getOverlappingInputs(0, range.getKey(), range.getValue());

            checkState(!levelInputs.isEmpty());
        }

        Compaction compaction = setupOtherInputs(level, levelInputs);
        return compaction;
    }

    private Compaction setupOtherInputs(int level, List<FileMetaData> levelInputs)
    {
        Map.Entry<InternalKey, InternalKey> range = getRange(levelInputs);
        InternalKey smallest = range.getKey();
        InternalKey largest = range.getValue();

        List<FileMetaData> levelUpInputs = getOverlappingInputs(level + 1, smallest, largest);

        // Get entire range covered by compaction
        range = getRange(levelInputs, levelUpInputs);
        InternalKey allStart = range.getKey();
        InternalKey allLimit = range.getValue();

        // See if we can grow the number of inputs in "level" without
        // changing the number of "level+1" files we pick up.
        if (!levelUpInputs.isEmpty()) {
            List<FileMetaData> expanded0 = getOverlappingInputs(level, allStart, allLimit);

            if (expanded0.size() > levelInputs.size()) {
                range = getRange(expanded0);
                InternalKey newStart = range.getKey();
                InternalKey newLimit = range.getValue();

                List<FileMetaData> expanded1 = getOverlappingInputs(level + 1, newStart, newLimit);
                if (expanded1.size() == levelUpInputs.size()) {
//              Log(options_->info_log,
//                  "Expanding@%d %d+%d to %d+%d\n",
//                  level,
//                  int(c->inputs_[0].size()),
//                  int(c->inputs_[1].size()),
//                  int(expanded0.size()),
//                  int(expanded1.size()));
                    smallest = newStart;
                    largest = newLimit;
                    levelInputs = expanded0;
                    levelUpInputs = expanded1;

                    range = getRange(levelInputs, levelUpInputs);
                    allStart = range.getKey();
                    allLimit = range.getValue();
                }
            }
        }

        // Compute the set of grandparent files that overlap this compaction
        // (parent == level+1; grandparent == level+2)
        List<FileMetaData> grandparents = ImmutableList.of();
        if (level + 2 < NUM_LEVELS) {
            grandparents = getOverlappingInputs(level + 2, allStart, allLimit);
        }

//        if (false) {
//            Log(options_ - > info_log, "Compacting %d '%s' .. '%s'",
//                    level,
//                    EscapeString(smallest.Encode()).c_str(),
//                    EscapeString(largest.Encode()).c_str());
//        }

        Compaction compaction = new Compaction(current, level, levelInputs, levelUpInputs, grandparents);

        // Update the place where we will do the next compaction for this level.
        // We update this immediately instead of waiting for the VersionEdit
        // to be applied so that if the compaction fails, we will try a different
        // key range next time.
        compactPointers.put(level, largest);
        compaction.getEdit().setCompactPointer(level, largest);

        return compaction;
    }

    List<FileMetaData> getOverlappingInputs(int level, InternalKey begin, InternalKey end)
    {
        ImmutableList.Builder<FileMetaData> files = ImmutableList.builder();
        Slice userBegin = begin.getUserKey();
        Slice userEnd = end.getUserKey();
        UserComparator userComparator = internalKeyComparator.getUserComparator();
        for (FileMetaData fileMetaData : current.getFiles(level)) {
            if (userComparator.compare(fileMetaData.getLargest().getUserKey(), userBegin) < 0 ||
                    userComparator.compare(fileMetaData.getSmallest().getUserKey(), userEnd) > 0) {
                // Either completely before or after range; skip it
            }
            else {
                files.add(fileMetaData);
            }
        }
        return files.build();
    }

    private Map.Entry<InternalKey, InternalKey> getRange(List<FileMetaData>... inputLists)
    {
        InternalKey smallest = null;
        InternalKey largest = null;
        for (List<FileMetaData> inputList : inputLists) {
            for (FileMetaData fileMetaData : inputList) {
                if (smallest == null) {
                    smallest = fileMetaData.getSmallest();
                    largest = fileMetaData.getLargest();
                }
                else {
                    if (internalKeyComparator.compare(fileMetaData.getSmallest(), smallest) < 0) {
                        smallest = fileMetaData.getSmallest();
                    }
                    if (internalKeyComparator.compare(fileMetaData.getLargest(), largest) > 0) {
                        largest = fileMetaData.getLargest();
                    }
                }
            }
        }
        return Maps.immutableEntry(smallest, largest);
    }

    public long getMaxNextLevelOverlappingBytes()
    {
        long result = 0;
        for (int level = 1; level < NUM_LEVELS; level++) {
            for (FileMetaData fileMetaData : current.getFiles(level)) {
                List<FileMetaData> overlaps = getOverlappingInputs(level + 1, fileMetaData.getSmallest(), fileMetaData.getLargest());
                long totalSize = 0;
                for (FileMetaData overlap : overlaps) {
                    totalSize += overlap.getFileSize();
                }
                result = Math.max(result, totalSize);
            }
        }
        return result;
    }

    /**
     * Builder是一个内部辅助类，其主要作用是：
     * 1 把一个MANIFEST记录的元信息应用到版本管理器VersionSet中；
     * 2 把当前的版本状态设置到一个Version对象中。
     */
    private static class Builder
    {
        private final VersionSet versionSet;
        private final Version baseVersion;
        private final List<LevelState> levels;

        private Builder(VersionSet versionSet, Version baseVersion)
        {
            this.versionSet = versionSet;
            this.baseVersion = baseVersion;

            levels = new ArrayList<>(baseVersion.numberOfLevels());
            for (int i = 0; i < baseVersion.numberOfLevels(); i++) {
                // 构建每一层的状态信息，包括添加、删除的文件
                levels.add(new LevelState(versionSet.internalKeyComparator));
            }
        }

        /**
         * 该函数将edit中的修改应用到当前状态中
         */
        public void apply(VersionEdit edit)
        {
            // 把edit记录的compaction点应用到当前状态
            for (Map.Entry<Integer, InternalKey> entry : edit.getCompactPointers().entrySet()) {
                Integer level = entry.getKey();
                InternalKey internalKey = entry.getValue();
                versionSet.compactPointers.put(level, internalKey);
            }

            // 把edit记录的已删除文件应用到当前状态
            for (Map.Entry<Integer, Long> entry : edit.getDeletedFiles().entries()) {
                Integer level = entry.getKey();
                Long fileNumber = entry.getValue();
                levels.get(level).deletedFiles.add(fileNumber);

            }

            // 把edit记录的新加文件应用到当前状态，这里会初始化文件的allowedSeeks值，以在文件被无谓seek指定次数后自动执行compaction，
            // 这里作者阐述了其设置规则。
            for (Map.Entry<Integer, FileMetaData> entry : edit.getNewFiles().entries()) {
                Integer level = entry.getKey();
                FileMetaData fileMetaData = entry.getValue();

                // 值allowedSeeks（引用计数）事关compaction的优化，其计算依据如下，首先假设：
                //   (1) 一次seek时间为10ms
                //   (2) 写入1MB数据的时间为10ms（100MB/s）
                //   (3) compact 1MB的数据需要执行25MB的IO：
                //         从本层读取1MB
                //         从下一层读取10-12MB（文件的key range边界可能是非对齐的）
                //         向下一层写入10-12MB
                // 这意味这25次seek的代价等同于compact 1MB的数据，也就是一次seek花费的时间大约相当于compact 40KB的数据。
                // 基于保守的角度考虑，对于每16KB的数据，我们允许它在触发compaction之前能做一次seek。
                int allowedSeeks = (int) (fileMetaData.getFileSize() / 16384);
                if (allowedSeeks < 100) {
                    allowedSeeks = 100;
                }
                fileMetaData.setAllowedSeeks(allowedSeeks);

                levels.get(level).deletedFiles.remove(fileMetaData.getNumber());
                levels.get(level).addedFiles.add(fileMetaData);
            }
        }

        /**
         * 把当前的状态存储到version中.
         * For循环遍历所有的level，把新加的文件和已存在的文件merge在一起，丢弃已删除的文件，结果保存在version中。
         * 对于level> 0，还要确保集合中的文件没有重合。
         */
        public void saveTo(Version version)
                throws IOException
        {
            FileMetaDataBySmallestKey cmp = new FileMetaDataBySmallestKey(versionSet.internalKeyComparator);
            for (int level = 0; level < baseVersion.numberOfLevels(); level++) {
                // 原文件集合
                Collection<FileMetaData> baseFiles = baseVersion.getFiles().asMap().get(level);
                if (baseFiles == null) {
                    baseFiles = ImmutableList.of();
                }
                SortedSet<FileMetaData> addedFiles = levels.get(level).addedFiles;
                if (addedFiles == null) {
                    addedFiles = ImmutableSortedSet.of();
                }

                // 将原文件和新加的文件，按顺序排好
                ArrayList<FileMetaData> sortedFiles = new ArrayList<>(baseFiles.size() + addedFiles.size());
                sortedFiles.addAll(baseFiles);
                sortedFiles.addAll(addedFiles);
                Collections.sort(sortedFiles, cmp);
                // 将文件merge到该level到version中
                for (FileMetaData fileMetaData : sortedFiles) {
                    maybeAddFile(version, level, fileMetaData);
                }
                // 确保version中level > 0的文件有序
                version.assertNoOverlappingFiles();
            }
        }

        /**
         * 该函数尝试将fileMetaData加入到levels[level]文件set中。
         * 要满足两个条件：
         * 1. 文件不能被删除，也就是不能在levels[level].deleted_files集合中；
         * 2. 保证文件之间的key是连续的，即基于比较器versionSet.internalKeyComparator，
         *    fileMetaData的smallest要大于levels[level]集合中最后一个文件的larges；
         * @param version
         * @param level
         * @param fileMetaData
         * @throws IOException
         */
        private void maybeAddFile(Version version, int level, FileMetaData fileMetaData)
                throws IOException
        {
            if (levels.get(level).deletedFiles.contains(fileMetaData.getNumber())) {
                // 如果文件已经被删除了，则什么都不需要做了
            }
            else {
                List<FileMetaData> files = version.getFiles(level);
                if (level > 0 && !files.isEmpty()) {
                    // 保证key的连续性
                    boolean filesOverlap = versionSet.internalKeyComparator.compare(files.get(files.size() - 1).getLargest(), fileMetaData.getSmallest()) >= 0;
                    if (filesOverlap) {
                        throw new IOException(String.format("Compaction is obsolete: Overlapping files %s and %s in level %s",
                                files.get(files.size() - 1).getNumber(),
                                fileMetaData.getNumber(), level));
                    }
                }
                version.addFile(level, fileMetaData);
            }
        }
        // 文件比较类，首先依照文件的min key，小的在前；如果min key相等则file number小的在前。
        private static class FileMetaDataBySmallestKey
                implements Comparator<FileMetaData>
        {
            private final InternalKeyComparator internalKeyComparator;

            private FileMetaDataBySmallestKey(InternalKeyComparator internalKeyComparator)
            {
                this.internalKeyComparator = internalKeyComparator;
            }

            @Override
            public int compare(FileMetaData f1, FileMetaData f2)
            {
                return ComparisonChain
                        .start()
                        .compare(f1.getSmallest(), f2.getSmallest(), internalKeyComparator)
                        .compare(f1.getNumber(), f2.getNumber())
                        .result();
            }
        }
        /**
         * 记录添加和删除的文件
         */
        private static class LevelState
        {
            // 保证添加文件的顺序是有效定义的
            private final SortedSet<FileMetaData> addedFiles;
            private final Set<Long> deletedFiles = new HashSet<Long>();
            // 构造函数创建比较类
            public LevelState(InternalKeyComparator internalKeyComparator)
            {
                // 比较类FileMetaDataBySmallestKey
                addedFiles = new TreeSet<FileMetaData>(new FileMetaDataBySmallestKey(internalKeyComparator));
            }

            @Override
            public String toString()
            {
                final StringBuilder sb = new StringBuilder();
                sb.append("LevelState");
                sb.append("{addedFiles=").append(addedFiles);
                sb.append(", deletedFiles=").append(deletedFiles);
                sb.append('}');
                return sb.toString();
            }
        }
    }
}
