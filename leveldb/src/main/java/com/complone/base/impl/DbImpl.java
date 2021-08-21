package com.complone.base.impl;

import com.complone.base.*;
import com.complone.base.utils.DataUnit;
import com.complone.base.utils.MergingIterator;
import com.complone.base.utils.Snappy;
import com.google.common.base.Throwables;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.complone.base.db.MemTable;
import com.complone.base.db.Slices;
import com.complone.base.include.Slice;
import com.complone.base.include.SliceInput;
import com.complone.base.include.SliceOutput;
import com.complone.base.table.BytewiseComparator;
import com.complone.base.table.CustomUserComparator;
import com.complone.base.table.TableBuilder;
import com.complone.base.table.UserComparator;

import java.io.*;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import com.complone.base.impl.WriteBatchImpl.Handler;

import static com.complone.base.db.Slices.readLengthPrefixedBytes;
import static com.complone.base.impl.SequenceNumber.MAX_SEQUENCE_NUMBER;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.complone.base.db.Slices.writeLengthPrefixedBytes;
import static com.complone.base.impl.DbConstants.*;
import static com.complone.base.impl.ValueType.DELETION;
import static com.complone.base.impl.ValueType.VALUE;
import static java.util.Objects.requireNonNull;

public class DbImpl
        implements DB
{
    private final Options options;
    private final File databaseDir;
    private final TableCache tableCache;
    private final DbLock dbLock;
    private final VersionSet versions;

    private final AtomicBoolean shuttingDown = new AtomicBoolean();
    private final ReentrantLock mutex = new ReentrantLock();
    private final Condition backgroundCondition = mutex.newCondition();

    private final List<Long> pendingOutputs = new ArrayList<>(); // todo

    private LogWriter log;

    private MemTable memTable;
    private MemTable immutableMemTable;

    private final InternalKeyComparator internalKeyComparator;

    private volatile Throwable backgroundException;
    private final ExecutorService compactionExecutor;
    private Future<?> backgroundCompaction;

    private ManualCompaction manualCompaction;

    public DbImpl(Options options, File databaseDir)
            throws IOException
    {
        requireNonNull(options, "options is null");
        requireNonNull(databaseDir, "databaseDir is null");
        this.options = options;

        if (this.options.compressionType() == CompressionType.SNAPPY && !Snappy.available()) {
            // 如果不支持SNAPPY压缩，则采取不压缩的方式
            this.options.compressionType(CompressionType.NONE);
        }

        this.databaseDir = databaseDir;

        // 如果已经指定了comparator，就使用CustomUserComparator
        DBComparator comparator = options.comparator();
        UserComparator userComparator;
        if (comparator != null) {
            userComparator = new CustomUserComparator(comparator);
        }
        else {
            userComparator = new BytewiseComparator();
        }
        internalKeyComparator = new InternalKeyComparator(userComparator);
        memTable = new MemTable(internalKeyComparator);
        immutableMemTable = null;

        ThreadFactory compactionThreadFactory = new ThreadFactoryBuilder()
                .setNameFormat("leveldb-compaction-%s")
                .setUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler()
                {
                    @Override
                    public void uncaughtException(Thread t, Throwable e)
                    {

                        System.out.printf("%s%n", t);
                        e.printStackTrace();
                    }
                })
                .build();
        compactionExecutor = Executors.newSingleThreadExecutor(compactionThreadFactory);

        // 在函数体中，创建TableCache和VersionSet。
        // 为其他预留10个文件，其余的都给TableCache.
        int tableCacheSize = options.maxOpenFiles() - 10;
        tableCache = new TableCache(databaseDir, tableCacheSize, new InternalUserComparator(internalKeyComparator), options.verifyChecksums());

        // create the version set

        // create the database dir if it does not already exist
        databaseDir.mkdirs();
        checkArgument(databaseDir.exists(), "Database directory '%s' does not exist and could not be created", databaseDir);
        checkArgument(databaseDir.isDirectory(), "Database directory '%s' is not a directory", databaseDir);
        // 采用ReentrantLock对文件加锁
        mutex.lock();
        try {
            // 对目录下对文件加锁
            dbLock = new DbLock(new File(databaseDir, Filename.lockFileName()));

            // 创建CURRENT文件
            File currentFile = new File(databaseDir, Filename.currentFileName());
            if (!currentFile.canRead()) {
                checkArgument(options.createIfMissing(), "Database '%s' does not exist and the create if missing option is disabled", databaseDir);
            }
            else {
                checkArgument(!options.errorIfExists(), "Database '%s' exists and the error if exists option is enabled", databaseDir);
            }

            // 初始化VersionSet
            versions = new VersionSet(databaseDir, tableCache, internalKeyComparator);

            // 安装当前版本
            versions.recover();

            // Recover from all newer log files than the ones named in the
            // descriptor (new log files may have been added by the previous
            // incarnation without registering them in the descriptor).
            //
            // Note that PrevLogNumber() is no longer used, but we pay
            // attention to it in case we are recovering a database
            // produced by an older version of leveldb.
            long minLogNumber = versions.getLogNumber();
            long previousLogNumber = versions.getPrevLogNumber();
            List<File> filenames = Filename.listFiles(databaseDir);

            List<Long> logs = new ArrayList<>();
            for (File filename : filenames) {
                Filename.FileInfo fileInfo = Filename.parseFileName(filename);

                if (fileInfo != null &&
                        fileInfo.getFileType() == Filename.FileType.LOG &&
                        ((fileInfo.getFileNumber() >= minLogNumber) || (fileInfo.getFileNumber() == previousLogNumber))) {
                    logs.add(fileInfo.getFileNumber());
                }
            }

            // Recover in the order in which the logs were generated
            VersionEdit edit = new VersionEdit();
            Collections.sort(logs);
            for (Long fileNumber : logs) {
                long maxSequence = recoverLogFile(fileNumber, edit);
                if (versions.getLastSequence() < maxSequence) {
                    versions.setLastSequence(maxSequence);
                }
            }

            // open transaction log
            long logFileNumber = versions.getNextFileNumber();
            this.log = Logs.createLogWriter(new File(databaseDir, Filename.logFileName(logFileNumber)), logFileNumber);
            edit.setLogNumber(log.getFileNumber());

            // apply recovered edits
            versions.logAndApply(edit);

            // cleanup unused files
            deleteObsoleteFiles();

            // schedule compactions
            maybeScheduleCompaction();
        }
        finally {
            mutex.unlock();
        }
    }

    @Override
    public void close()
    {
        if (shuttingDown.getAndSet(true)) {
            return;
        }

        mutex.lock();
        try {
            while (backgroundCompaction != null) {
                backgroundCondition.awaitUninterruptibly();
            }
        }
        finally {
            mutex.unlock();
        }

        compactionExecutor.shutdown();
        try {
            compactionExecutor.awaitTermination(1, TimeUnit.DAYS);
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        try {
            versions.destroy();
        }
        catch (IOException ignored) {
        }
        try {
            log.close();
        }
        catch (IOException ignored) {
        }
        tableCache.close();
        dbLock.release();
    }

    @Override
    public String getProperty(String name)
    {
        checkBackgroundException();
        return null;
    }

    /**
     * 删除无引用的文件
     */
    private void deleteObsoleteFiles()
    {
        checkState(mutex.isHeldByCurrentThread());

        // 所有sstable文件集
        List<Long> live = new ArrayList<>(this.pendingOutputs);
        for (FileMetaData fileMetaData : versions.getLiveFiles()) {
            live.add(fileMetaData.getNumber());
        }

        for (File file : Filename.listFiles(databaseDir)) {
            Filename.FileInfo fileInfo = Filename.parseFileName(file);
            if (fileInfo == null) {
                continue;
            }
            long number = fileInfo.getFileNumber();
            boolean keep = true;
            switch (fileInfo.getFileType()) {
                case LOG:
                    keep = ((number >= versions.getLogNumber()) ||
                            (number == versions.getPrevLogNumber()));
                    break;
                case DESCRIPTOR:
                    keep = (number >= versions.getManifestFileNumber());
                    break;
                case TABLE:
                    keep = live.contains(number);
                    break;
                case TEMP:
                    keep = live.contains(number);
                    break;
                case CURRENT:
                case DB_LOCK:
                case INFO_LOG:
                    keep = true;
                    break;
            }

            if (!keep) {
                if (fileInfo.getFileType() == Filename.FileType.TABLE) {
                    tableCache.evict(number);
                }
                // todo info logging system needed
                file.delete();
            }
        }
    }

    public void flushMemTable()
    {
        mutex.lock();
        try {
            // force compaction
            makeRoomForWrite(true);

            // todo bg_error code
            while (immutableMemTable != null) {
                backgroundCondition.awaitUninterruptibly();
            }

        }
        finally {
            mutex.unlock();
        }
    }

    public void compactRange(int level, Slice start, Slice end)
    {
        checkArgument(level >= 0, "level is negative");
        checkArgument(level + 1 < NUM_LEVELS, "level is greater than or equal to %s", NUM_LEVELS);
        requireNonNull(start, "start is null");
        requireNonNull(end, "end is null");

        mutex.lock();
        try {
            while (this.manualCompaction != null) {
                backgroundCondition.awaitUninterruptibly();
            }
            ManualCompaction manualCompaction = new ManualCompaction(level, start, end);
            this.manualCompaction = manualCompaction;

            maybeScheduleCompaction();

            while (this.manualCompaction == manualCompaction) {
                backgroundCondition.awaitUninterruptibly();
            }
        }
        finally {
            mutex.unlock();
        }

    }

    /**
     * 判断后台线程是否已经启动和一些其他的错误判断，如果未启动则启动后台compaction线程
     */
    private void maybeScheduleCompaction()
    {
        checkState(mutex.isHeldByCurrentThread());

        if (backgroundCompaction != null) {
            // 已经在后台执行compact
        }
        else if (shuttingDown.get()) {
            // DB 已经被关闭了
        }
        else if (immutableMemTable == null &&
                manualCompaction == null &&
                !versions.needsCompaction()) {
            // No work to be done
        }
        else {
            backgroundCompaction = compactionExecutor.submit(new Callable<Void>()
            {
                @Override
                public Void call()
                        throws Exception
                {
                    try {
                        // 启动后台compact线程
                        backgroundCall();
                    }
                    catch (DatabaseShutdownException ignored) {
                    }
                    catch (Throwable e) {
                        backgroundException = e;
                    }
                    return null;
                }
            });
        }
    }

    public void checkBackgroundException()
    {
        Throwable e = backgroundException;
        if (e != null) {
            throw new BackgroundProcessingException(e);
        }
    }

    /**
     * 启动后台compact线程
     * @throws IOException
     */
    private void backgroundCall()
            throws IOException
    {
        mutex.lock();
        try {
            if (backgroundCompaction == null) {
                return;
            }

            try {
                //TODO 缓存取出topicID对应的最大消费位点offset
                if (!shuttingDown.get()) {
                    backgroundCompaction();
                }
            }
            finally {
                backgroundCompaction = null;
            }
        }
        finally {
            try {
                // 如果之前的compact产生了太多文件的话，就在这里再进行一次compact
                maybeScheduleCompaction();
            }
            finally {
                try {
                    backgroundCondition.signalAll();
                }
                finally {
                    mutex.unlock();
                }
            }
        }
    }

    // compact的核心实现
    private void backgroundCompaction()
            throws IOException
    {
        checkState(mutex.isHeldByCurrentThread());

        compactMemTableInternal();

        Compaction compaction;
        if (manualCompaction != null) {
            compaction = versions.compactRange(manualCompaction.level,
                    new InternalKey(manualCompaction.begin, MAX_SEQUENCE_NUMBER, VALUE),
                    new InternalKey(manualCompaction.end, 0, DELETION));
        }
        else {
            compaction = versions.pickCompaction();
        }

        if (compaction == null) {
            // no compaction
        }
        else if (manualCompaction == null && compaction.isTrivialMove()) {
            // Move file to next level
            checkState(compaction.getLevelInputs().size() == 1);
            FileMetaData fileMetaData = compaction.getLevelInputs().get(0);
            compaction.getEdit().deleteFile(compaction.getLevel(), fileMetaData.getNumber());
            compaction.getEdit().addFile(compaction.getLevel() + 1, fileMetaData);
            versions.logAndApply(compaction.getEdit());
            // log
        }
        else {
            CompactionState compactionState = new CompactionState(compaction);
            doCompactionWork(compactionState);
            cleanupCompaction(compactionState);
        }

        // manual compaction complete
        if (manualCompaction != null) {
            manualCompaction = null;
        }
    }

    private void cleanupCompaction(CompactionState compactionState)
    {
        checkState(mutex.isHeldByCurrentThread());

        if (compactionState.builder != null) {
            compactionState.builder.abandon();
        }
        else {
            checkArgument(compactionState.outfile == null);
        }

        for (FileMetaData output : compactionState.outputs) {
            pendingOutputs.remove(output.getNumber());
        }
    }

    private long recoverLogFile(long fileNumber, VersionEdit edit)
            throws IOException
    {
        checkState(mutex.isHeldByCurrentThread());
        File file = new File(databaseDir, Filename.logFileName(fileNumber));
        try (FileInputStream fis = new FileInputStream(file);
             FileChannel channel = fis.getChannel()) {
            LogMonitor logMonitor = LogMonitors.logMonitor();
            LogReader logReader = new LogReader(channel, logMonitor, true, 0);

            // Log(options_.info_log, "Recovering log #%llu", (unsigned long long) log_number);

            // Read all the records and add to a memtable
            long maxSequence = 0;
            MemTable memTable = null;
            for (Slice record = logReader.readRecord(); record != null; record = logReader.readRecord()) {
                SliceInput sliceInput = record.input();
                // read header
                if (sliceInput.available() < 12) {
                    logMonitor.corruption(sliceInput.available(), "log record too small");
                    continue;
                }
                long sequenceBegin = sliceInput.readLong();
                int updateSize = sliceInput.readInt();

                // read entries
                WriteBatchImpl writeBatch = readWriteBatch(sliceInput, updateSize);

                // apply entries to memTable
                if (memTable == null) {
                    memTable = new MemTable(internalKeyComparator);
                }
                writeBatch.forEach(new InsertIntoHandler(memTable, sequenceBegin));

                // update the maxSequence
                long lastSequence = sequenceBegin + updateSize - 1;
                if (lastSequence > maxSequence) {
                    maxSequence = lastSequence;
                }

                // flush mem table if necessary
                if (memTable.approximateMemoryUsage() > options.writeBufferSize()) {
                    writeLevel0Table(memTable, edit, null);
                    memTable = null;
                }
            }

            // flush mem table
            if (memTable != null && !memTable.isEmpty()) {
                writeLevel0Table(memTable, edit, null);
            }

            return maxSequence;
        }
    }

    @Override
    public byte[] get(byte[] key)
            throws DBException
    {
        return get(key, new ReadOptions());
    }

    @Override
    public byte[] get(byte[] key, ReadOptions options)
            throws DBException
    {
        checkBackgroundException();
        LookupKey lookupKey;
        mutex.lock();
        try {
            SnapshotImpl snapshot = getSnapshot(options);
            lookupKey = new LookupKey(Slices.wrappedBuffer(key), snapshot.getLastSequence());

            LookupResult lookupResult = memTable.get(lookupKey);
            if (lookupResult != null) {
                Slice value = lookupResult.getValue();
                if (value == null) {
                    return null;
                }
                return value.getBytes();
            }
            if (immutableMemTable != null) {
                lookupResult = immutableMemTable.get(lookupKey);
                if (lookupResult != null) {
                    Slice value = lookupResult.getValue();
                    if (value == null) {
                        return null;
                    }
                    return value.getBytes();
                }
            }
        }
        finally {
            mutex.unlock();
        }

        LookupResult lookupResult = versions.get(lookupKey);

        mutex.lock();
        try {
            if (versions.needsCompaction()) {
                maybeScheduleCompaction();
            }
        }
        finally {
            mutex.unlock();
        }

        if (lookupResult != null) {
            Slice value = lookupResult.getValue();
            if (value != null) {
                return value.getBytes();
            }
        }
        return null;
    }

    @Override
    public void put(byte[] key, byte[] value)
            throws DBException
    {
        put(key, value, new WriteOptions());
    }

    @Override
    public Snapshot put(byte[] key, byte[] value, WriteOptions options)
            throws DBException
    {
        return writeInternal(new WriteBatchImpl().put(key, value), options);
    }

    @Override
    public void delete(byte[] key)
            throws DBException
    {
        writeInternal(new WriteBatchImpl().delete(key), new WriteOptions());
    }

    @Override
    public Snapshot delete(byte[] key, WriteOptions options)
            throws DBException
    {
        return writeInternal(new WriteBatchImpl().delete(key), options);
    }

    @Override
    public void write(WriteBatch updates)
            throws DBException
    {
        writeInternal((WriteBatchImpl) updates, new WriteOptions());
    }

    @Override
    public Snapshot write(WriteBatch updates, WriteOptions options)
            throws DBException
    {
        return writeInternal((WriteBatchImpl) updates, options);
    }

    public Snapshot writeInternal(WriteBatchImpl updates, WriteOptions options)
            throws DBException
    {
        checkBackgroundException();
        mutex.lock();
        try {
            long sequenceEnd;
            if (updates.size() != 0) {
                makeRoomForWrite(false);

                // 获取last sequence
                long sequenceBegin = versions.getLastSequence() + 1;
                // updates.size指的是key-value对
                sequenceEnd = sequenceBegin + updates.size() - 1;

                // 修改version的last sequence
                versions.setLastSequence(sequenceEnd);

                // Log write
                Slice record = writeWriteBatch(updates, sequenceBegin);
                try {
                    log.addRecord(record, options.sync());
                }
                catch (IOException e) {
                    throw Throwables.propagate(e);
                }

                // 更新 memtable
                updates.forEach(new InsertIntoHandler(memTable, sequenceBegin));
            }
            else {
                sequenceEnd = versions.getLastSequence();
            }

            if (options.snapshot()) {
                return new SnapshotImpl(versions.getCurrent(), sequenceEnd);
            }
            else {
                return null;
            }
        }
        finally {
            mutex.unlock();
        }
    }

    @Override
    public WriteBatch createWriteBatch()
    {
        checkBackgroundException();
        return new WriteBatchImpl();
    }

    @Override
    public SeekingIteratorAdapter iterator()
    {
        return iterator(new ReadOptions());
    }

    @Override
    public SeekingIteratorAdapter iterator(ReadOptions options)
    {
        checkBackgroundException();
        mutex.lock();
        try {
            DbIterator rawIterator = internalIterator();

            // filter any entries not visible in our snapshot
            SnapshotImpl snapshot = getSnapshot(options);
            SnapshotSeekingIterator snapshotIterator = new SnapshotSeekingIterator(rawIterator, snapshot, internalKeyComparator.getUserComparator());
            return new SeekingIteratorAdapter(snapshotIterator);
        }
        finally {
            mutex.unlock();
        }
    }

    SeekingIterable<InternalKey, Slice> internalIterable()
    {
        return new SeekingIterable<InternalKey, Slice>()
        {
            @Override
            public DbIterator iterator()
            {
                return internalIterator();
            }
        };
    }

    DbIterator internalIterator()
    {
        mutex.lock();
        try {
            // merge together the memTable, immutableMemTable, and tables in version set
            MemTable.MemTableIterator iterator = null;
            if (immutableMemTable != null) {
                iterator = immutableMemTable.iterator();
            }
            Version current = versions.getCurrent();
            return new DbIterator(memTable.iterator(), iterator, current.getLevel0Files(), current.getLevelIterators(), internalKeyComparator);
        }
        finally {
            mutex.unlock();
        }
    }

    @Override
    public Snapshot getSnapshot()
    {
        checkBackgroundException();
        mutex.lock();
        try {
            return new SnapshotImpl(versions.getCurrent(), versions.getLastSequence());
        }
        finally {
            mutex.unlock();
        }
    }

    private SnapshotImpl getSnapshot(ReadOptions options)
    {
        SnapshotImpl snapshot;
        if (options.snapshot() != null) {
            snapshot = (SnapshotImpl) options.snapshot();
        }
        else {
            snapshot = new SnapshotImpl(versions.getCurrent(), versions.getLastSequence());
            snapshot.close(); // To avoid holding the snapshot active..
        }
        return snapshot;
    }

    private void makeRoomForWrite(boolean force)
    {
        checkState(mutex.isHeldByCurrentThread());

        boolean allowDelay = !force;

        while (true) {

            if (allowDelay && versions.numberOfFilesInLevel(0) > L0_SLOWDOWN_WRITES_TRIGGER) {
                /**
                 * 当L0的文件数量要达到阈值的时候，我们每次写入都延迟1ms，
                 * 这样可以为后台的compaction腾出一定的cpu（当后台compaction
                 * 和当前线程是使用的一个内核的时候）这样可以降低写入延迟的方差
                 * 因为延迟被分摊到多个写上面，而不是在几个甚至一个写的时候
                 */
                try {
                    mutex.unlock();
                    Thread.sleep(1);
                }
                catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException(e);
                }
                finally {
                    mutex.lock();
                }

                // 每次写只允许延迟一次
                allowDelay = false;
            }
            else if (!force && memTable.approximateMemoryUsage() <= options.writeBufferSize()) {
                // 当前memtable的占用量未达到阈值
                break;
            }
            else if (immutableMemTable != null) {
                /**
                 * 上一次memtable的compaction尚未结束，等待后台compaction完成
                 * 因为compaction的过程为 mem ->immutableMemTable 完成后删除immutableMemTable
                 *
                 * 线程在调用condition.await()后处于await状态，此时调用thread.interrupt()会报错
                 * 但是使用condition.awaitUninterruptibly()后，调用thread.interrupt(0则不会报错
                 */
                backgroundCondition.awaitUninterruptibly();
            }
            else if (versions.numberOfFilesInLevel(0) >= L0_STOP_WRITES_TRIGGER) {
                // level 0的文件数量超过阈值，等待后台compaction完成
                backgroundCondition.awaitUninterruptibly();
            }
            else {
                // memtable达到阈值，新生成日志和memtable，并将原先的mem转化为imm给后台compact
                checkState(versions.getPrevLogNumber() == 0);

                // 关闭现在的log文件
                try {
                    log.close();
                }
                catch (IOException e) {
                    throw new RuntimeException("Unable to close log file " + log.getFile(), e);
                }

                // 打开一个新的log文件
                long logNumber = versions.getNextFileNumber();
                try {
                    this.log = Logs.createLogWriter(new File(databaseDir, Filename.logFileName(logNumber)), logNumber);
                }
                catch (IOException e) {
                    throw new RuntimeException("Unable to open new log file " +
                            new File(databaseDir, Filename.logFileName(logNumber)).getAbsoluteFile(), e);
                }

                // 将当前的memtable赋值给immutableMemTable，新建memTable
                immutableMemTable = memTable;
                memTable = new MemTable(internalKeyComparator);

                // Do not force another compaction there is space available
                force = false;
                //触发后台compaction
                maybeScheduleCompaction();
            }
        }
    }

    public void compactMemTable()
            throws IOException
    {
        mutex.lock();
        try {
            compactMemTableInternal();
        }
        finally {
            mutex.unlock();
        }
    }

    /**
     * immutableMemTable转换为sstable
     * @throws IOException
     */
    private void compactMemTableInternal()
            throws IOException
    {
        checkState(mutex.isHeldByCurrentThread());
        // 如果immutableMemTable不存在，则直接返回
        if (immutableMemTable == null) {
            return;
        }

        try {
            // 将immutableMemTable转换为sstable
            VersionEdit edit = new VersionEdit();
            Version base = versions.getCurrent();
            writeLevel0Table(immutableMemTable, edit, base);

            if (shuttingDown.get()) {
                throw new DatabaseShutdownException("Database shutdown during memtable compaction");
            }

            // 替换immutableMemTable
            edit.setPreviousLogNumber(0);
            edit.setLogNumber(log.getFileNumber());  // Earlier logs no longer needed
            versions.logAndApply(edit);

            immutableMemTable = null;

            deleteObsoleteFiles();
        }
        finally {
            // 唤醒所有的等待线程
            backgroundCondition.signalAll();
        }
    }
    // Minor Compaction将memtable生成一个level 0文件
    private void writeLevel0Table(MemTable mem, VersionEdit edit, Version base)
            throws IOException
    {
        checkState(mutex.isHeldByCurrentThread());

        // 跳过空的memtable
        if (mem.isEmpty()) {
            return;
        }

        // 产生一个新的file number，用于产生新的sstable
        long fileNumber = versions.getNextFileNumber();
        pendingOutputs.add(fileNumber);
        mutex.unlock();
        FileMetaData meta;
        try {
            meta = buildTable(mem, fileNumber);
        }
        finally {
            mutex.lock();
        }
        // 创建成功后，移除任务
        pendingOutputs.remove(fileNumber);

        // Note that if file size is zero, the file has been deleted and
        // should not be added to the manifest.
        int level = 0;
        if (meta != null && meta.getFileSize() > 0) {
            Slice minUserKey = meta.getSmallest().getUserKey();
            Slice maxUserKey = meta.getLargest().getUserKey();
            if (base != null) {
                level = base.pickLevelForMemTableOutput(minUserKey, maxUserKey);
            }
            edit.addFile(level, meta);
        }
    }

    private FileMetaData buildTable(SeekingIterable<InternalKey, Slice> data, long fileNumber)
            throws IOException
    {
        File file = new File(databaseDir, Filename.tableFileName(fileNumber));
        try {
            InternalKey smallest = null;
            InternalKey largest = null;
            FileChannel channel = new FileOutputStream(file).getChannel();
            try {
                TableBuilder tableBuilder = new TableBuilder(options, channel, new InternalUserComparator(internalKeyComparator));

                for (Map.Entry<InternalKey, Slice> entry : data) {
                    // update keys
                    InternalKey key = entry.getKey();
                    if (smallest == null) {
                        smallest = key;
                    }
                    largest = key;

                    tableBuilder.add(key.encode(), entry.getValue());
                }

                tableBuilder.finish();
            }
            finally {
                try {
                    channel.force(true);
                }
                finally {
                    channel.close();
                }
            }

            if (smallest == null) {
                return null;
            }
            FileMetaData fileMetaData = new FileMetaData(fileNumber, file.length(), smallest, largest);

            // verify table can be opened
            tableCache.newIterator(fileMetaData);

            pendingOutputs.remove(fileNumber);

            return fileMetaData;

        }
        catch (IOException e) {
            file.delete();
            throw e;
        }
    }

    private void doCompactionWork(CompactionState compactionState)
            throws IOException
    {
        checkState(mutex.isHeldByCurrentThread());
        checkArgument(versions.numberOfBytesInLevel(compactionState.getCompaction().getLevel()) > 0);
        checkArgument(compactionState.builder == null);
        checkArgument(compactionState.outfile == null);

        // 将snapshot相关的内容记录到compact信息中
        compactionState.smallestSnapshot = versions.getLastSequence();

        // 加锁
        mutex.unlock();
        try {
            MergingIterator iterator = versions.makeInputIterator(compactionState.compaction);

            Slice currentUserKey = null;
            boolean hasCurrentUserKey = false;

            long lastSequenceForKey = MAX_SEQUENCE_NUMBER;
            while (iterator.hasNext() && !shuttingDown.get()) {
                // always give priority to compacting the current mem table
                mutex.lock();
                try {
                    compactMemTableInternal();
                }
                finally {
                    mutex.unlock();
                }

                InternalKey key = iterator.peek().getKey();
                if (compactionState.compaction.shouldStopBefore(key) && compactionState.builder != null) {
                    finishCompactionOutputFile(compactionState);
                }

                // Handle key/value, add to state, etc.
                boolean drop = false;
                // todo if key doesn't parse (it is corrupted),
                if (false /*!ParseInternalKey(key, &ikey)*/) {
                    // do not hide error keys
                    currentUserKey = null;
                    hasCurrentUserKey = false;
                    lastSequenceForKey = MAX_SEQUENCE_NUMBER;
                }
                else {
                    if (!hasCurrentUserKey || internalKeyComparator.getUserComparator().compare(key.getUserKey(), currentUserKey) != 0) {
                        // First occurrence of this user key
                        currentUserKey = key.getUserKey();
                        hasCurrentUserKey = true;
                        lastSequenceForKey = MAX_SEQUENCE_NUMBER;
                    }

                    if (lastSequenceForKey <= compactionState.smallestSnapshot) {
                        // Hidden by an newer entry for same user key
                        drop = true; // (A)
                    }
                    else if (key.getValueType() == DELETION &&
                            key.getSequenceNumber() <= compactionState.smallestSnapshot &&
                            compactionState.compaction.isBaseLevelForKey(key.getUserKey())) {
                        // For this user key:
                        // (1) there is no data in higher levels
                        // (2) data in lower levels will have larger sequence numbers
                        // (3) data in layers that are being compacted here and have
                        //     smaller sequence numbers will be dropped in the next
                        //     few iterations of this loop (by rule (A) above).
                        // Therefore this deletion marker is obsolete and can be dropped.
                        drop = true;
                    }

                    lastSequenceForKey = key.getSequenceNumber();
                }

                if (!drop) {
                    // Open output file if necessary
                    if (compactionState.builder == null) {
                        openCompactionOutputFile(compactionState);
                    }
                    if (compactionState.builder.getEntryCount() == 0) {
                        compactionState.currentSmallest = key;
                    }
                    compactionState.currentLargest = key;
                    compactionState.builder.add(key.encode(), iterator.peek().getValue());

                    // Close output file if it is big enough
                    if (compactionState.builder.getFileSize() >=
                            compactionState.compaction.getMaxOutputFileSize()) {
                        finishCompactionOutputFile(compactionState);
                    }
                }
                iterator.next();
            }

            if (shuttingDown.get()) {
                throw new DatabaseShutdownException("DB shutdown during compaction");
            }
            if (compactionState.builder != null) {
                finishCompactionOutputFile(compactionState);
            }
        }
        finally {
            mutex.lock();
        }

        // todo port CompactionStats code

        installCompactionResults(compactionState);
    }

    private void openCompactionOutputFile(CompactionState compactionState)
            throws FileNotFoundException
    {
        requireNonNull(compactionState, "compactionState is null");
        checkArgument(compactionState.builder == null, "compactionState builder is not null");

        mutex.lock();
        try {
            long fileNumber = versions.getNextFileNumber();
            pendingOutputs.add(fileNumber);
            compactionState.currentFileNumber = fileNumber;
            compactionState.currentFileSize = 0;
            compactionState.currentSmallest = null;
            compactionState.currentLargest = null;

            File file = new File(databaseDir, Filename.tableFileName(fileNumber));
            compactionState.outfile = new FileOutputStream(file).getChannel();
            compactionState.builder = new TableBuilder(options, compactionState.outfile, new InternalUserComparator(internalKeyComparator));
        }
        finally {
            mutex.unlock();
        }
    }

    private void finishCompactionOutputFile(CompactionState compactionState)
            throws IOException
    {
        requireNonNull(compactionState, "compactionState is null");
        checkArgument(compactionState.outfile != null);
        checkArgument(compactionState.builder != null);

        long outputNumber = compactionState.currentFileNumber;
        checkArgument(outputNumber != 0);

        long currentEntries = compactionState.builder.getEntryCount();
        compactionState.builder.finish();

        long currentBytes = compactionState.builder.getFileSize();
        compactionState.currentFileSize = currentBytes;
        compactionState.totalBytes += currentBytes;

        FileMetaData currentFileMetaData = new FileMetaData(compactionState.currentFileNumber,
                compactionState.currentFileSize,
                compactionState.currentSmallest,
                compactionState.currentLargest);
        compactionState.outputs.add(currentFileMetaData);

        compactionState.builder = null;

        compactionState.outfile.force(true);
        compactionState.outfile.close();
        compactionState.outfile = null;

        if (currentEntries > 0) {
            // Verify that the table is usable
            tableCache.newIterator(outputNumber);
        }
    }

    private void installCompactionResults(CompactionState compact)
            throws IOException
    {
        checkState(mutex.isHeldByCurrentThread());

        // Add compaction outputs
        compact.compaction.addInputDeletions(compact.compaction.getEdit());
        int level = compact.compaction.getLevel();
        for (FileMetaData output : compact.outputs) {
            compact.compaction.getEdit().addFile(level + 1, output);
            pendingOutputs.remove(output.getNumber());
        }

        try {
            versions.logAndApply(compact.compaction.getEdit());
            deleteObsoleteFiles();
        }
        catch (IOException e) {
            // Compaction failed for some reason.  Simply discard the work and try again later.

            // Discard any files we may have created during this failed compaction
            for (FileMetaData output : compact.outputs) {
                File file = new File(databaseDir, Filename.tableFileName(output.getNumber()));
                file.delete();
            }
            compact.outputs.clear();
        }
    }

    int numberOfFilesInLevel(int level)
    {
        return versions.getCurrent().numberOfFilesInLevel(level);
    }

    @Override
    public long[] getApproximateSizes(Range... ranges)
    {
        requireNonNull(ranges, "ranges is null");
        long[] sizes = new long[ranges.length];
        for (int i = 0; i < ranges.length; i++) {
            Range range = ranges[i];
            sizes[i] = getApproximateSizes(range);
        }
        return sizes;
    }

    public long getApproximateSizes(Range range)
    {
        Version v = versions.getCurrent();

        InternalKey startKey = new InternalKey(Slices.wrappedBuffer(range.start()), MAX_SEQUENCE_NUMBER, VALUE);
        InternalKey limitKey = new InternalKey(Slices.wrappedBuffer(range.limit()), MAX_SEQUENCE_NUMBER, VALUE);
        long startOffset = v.getApproximateOffsetOf(startKey);
        long limitOffset = v.getApproximateOffsetOf(limitKey);

        return (limitOffset >= startOffset ? limitOffset - startOffset : 0);
    }

    public long getMaxNextLevelOverlappingBytes()
    {
        return versions.getMaxNextLevelOverlappingBytes();
    }

    private static class CompactionState
    {
        private final Compaction compaction;

        private final List<FileMetaData> outputs = new ArrayList<>();

        private long smallestSnapshot;

        // State kept for output being generated
        private FileChannel outfile;
        private TableBuilder builder;

        // Current file being generated
        private long currentFileNumber;
        private long currentFileSize;
        private InternalKey currentSmallest;
        private InternalKey currentLargest;

        private long totalBytes;

        private CompactionState(Compaction compaction)
        {
            this.compaction = compaction;
        }

        public Compaction getCompaction()
        {
            return compaction;
        }
    }

    private static class ManualCompaction
    {
        private final int level;
        private final Slice begin;
        private final Slice end;

        private ManualCompaction(int level, Slice begin, Slice end)
        {
            this.level = level;
            this.begin = begin;
            this.end = end;
        }
    }

    private WriteBatchImpl readWriteBatch(SliceInput record, int updateSize)
            throws IOException
    {
        WriteBatchImpl writeBatch = new WriteBatchImpl();
        int entries = 0;
        while (record.isReadable()) {
            entries++;
            ValueType valueType = ValueType.getValueTypeByPersistentId(record.readByte());
            if (valueType == VALUE) {
                Slice key = readLengthPrefixedBytes(record);
                Slice value = readLengthPrefixedBytes(record);
                writeBatch.put(key, value);
            }
            else if (valueType == DELETION) {
                Slice key = readLengthPrefixedBytes(record);
                writeBatch.delete(key);
            }
            else {
                throw new IllegalStateException("Unexpected value type " + valueType);
            }
        }

        if (entries != updateSize) {
            throw new IOException(String.format("Expected %d entries in log record but found %s entries", updateSize, entries));
        }

        return writeBatch;
    }

    private Slice writeWriteBatch(WriteBatchImpl updates, long sequenceBegin)
    {
        Slice record = Slices.allocate(DataUnit.LONG_UNIT + DataUnit.INT_UNIT + updates.getApproximateSize());
        final SliceOutput sliceOutput = record.output();
        sliceOutput.writeLong(sequenceBegin);
        sliceOutput.writeInt(updates.size());
        updates.forEach(new Handler()
        {
            @Override
            public void put(Slice key, Slice value)
            {
                sliceOutput.writeByte(VALUE.getPersistentId());
                writeLengthPrefixedBytes(sliceOutput, key);
                writeLengthPrefixedBytes(sliceOutput, value);
            }

            @Override
            public void delete(Slice key)
            {
                sliceOutput.writeByte(DELETION.getPersistentId());
                writeLengthPrefixedBytes(sliceOutput, key);
            }
        });
        return record.slice(0, sliceOutput.size());
    }

    private static class InsertIntoHandler
            implements Handler
    {
        private long sequence;
        private final MemTable memTable;

        public InsertIntoHandler(MemTable memTable, long sequenceBegin)
        {
            this.memTable = memTable;
            this.sequence = sequenceBegin;
        }

        @Override
        public void put(Slice key, Slice value)
        {
            memTable.add(sequence++, VALUE, key, value);
        }

        @Override
        public void delete(Slice key)
        {
            memTable.add(sequence++, DELETION, key, Slices.EMPTY_SLICE);
        }
    }

    public static class DatabaseShutdownException
            extends DBException
    {
        public DatabaseShutdownException()
        {
        }

        public DatabaseShutdownException(String message)
        {
            super(message);
        }
    }

    public static class BackgroundProcessingException
            extends DBException
    {
        public BackgroundProcessingException(Throwable cause)
        {
            super(cause);
        }
    }

    private final Object suspensionMutex = new Object();
    private int suspensionCounter;

    @Override
    public void suspendCompactions()
            throws InterruptedException
    {
        compactionExecutor.execute(new Runnable()
        {
            @Override
            public void run()
            {
                try {
                    synchronized (suspensionMutex) {
                        suspensionCounter++;
                        suspensionMutex.notifyAll();
                        while (suspensionCounter > 0 && !compactionExecutor.isShutdown()) {
                            suspensionMutex.wait(500);
                        }
                    }
                }
                catch (InterruptedException e) {
                }
            }
        });
        synchronized (suspensionMutex) {
            while (suspensionCounter < 1) {
                suspensionMutex.wait();
            }
        }
    }

    @Override
    public void resumeCompactions()
    {
        synchronized (suspensionMutex) {
            suspensionCounter--;
            suspensionMutex.notifyAll();
        }
    }

    @Override
    public void compactRange(byte[] begin, byte[] end)
            throws DBException
    {
        throw new UnsupportedOperationException("Not yet implemented");
    }
}
