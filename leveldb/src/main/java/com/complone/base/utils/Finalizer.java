package com.complone.base.utils;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

import java.lang.ref.PhantomReference;
import java.lang.ref.ReferenceQueue;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public class Finalizer<T>
{
    public static final FinalizerMonitor IGNORE_FINALIZER_MONITOR = new FinalizerMonitor()
    {
        @Override
        public void unexpectedException(Throwable throwable)
        {
        }
    };

    private final int threads;
    private final FinalizerMonitor monitor;

    private final ConcurrentHashMap<FinalizerPhantomReference<T>, Object> references = new ConcurrentHashMap<>();
    private final ReferenceQueue<T> referenceQueue = new ReferenceQueue<>();
    private final AtomicBoolean destroyed = new AtomicBoolean();
    private ExecutorService executor;

    public Finalizer()
    {
        this(1, IGNORE_FINALIZER_MONITOR);
    }

    public Finalizer(int threads)
    {
        this(1, IGNORE_FINALIZER_MONITOR);
    }

    public Finalizer(int threads, FinalizerMonitor monitor)
    {
        this.monitor = monitor;
        checkArgument(threads >= 1, "threads must be at least 1");
        this.threads = threads;
    }

    public synchronized void addCleanup(T item, Callable<?> cleanup)
    {
        requireNonNull(item, "item is null");
        requireNonNull(cleanup, "cleanup is null");
        checkState(!destroyed.get(), "%s is destroyed", getClass().getName());

        if (executor == null) {
            // create executor
            ThreadFactory threadFactory = new ThreadFactoryBuilder()
                    .setNameFormat("FinalizerQueueProcessor-%d")
                    .setDaemon(true)
                    .build();
            // 创建threads个线程，每个线程由线程工厂创建
            executor = Executors.newFixedThreadPool(threads, threadFactory);

            for (int i = 0; i < threads; i++) {
                // 将任务交给线程池执行
                executor.submit(new FinalizerQueueProcessor());
            }
        }

        // 创建item对象的虚引用，并配套引用队列，可以通过引用队列获得对象，并在垃圾回收之前对对象做处理
        FinalizerPhantomReference<T> reference = new FinalizerPhantomReference<>(item, referenceQueue, cleanup);

        // 当item已经被回收之后，在references中获取这个对象的虚引用，将虚引用对象加入到ConcurrentHashMap中
        references.put(reference, Boolean.TRUE);
    }

    public synchronized void destroy()
    {
        destroyed.set(true);
        if (executor != null) {
            executor.shutdownNow();
        }
        for (FinalizerPhantomReference<T> r : references.keySet()) {
            try {
                // 在销毁Finalizer之前，清除引用队列里的虚引用对象
                r.cleanup();
            }
            catch (Exception e) {
            }
        }
    }

    public interface FinalizerMonitor
    {
        void unexpectedException(Throwable throwable);
    }

    private static class FinalizerPhantomReference<T>
            extends PhantomReference<T>
    {
        private final AtomicBoolean cleaned = new AtomicBoolean(false);
        private final Callable<?> cleanup;
        // 虚引用的对象：referent，引用队列：queue
        // 下面构造函数的目的是创建虚引用对象referent，并将它关联到引用队列queue。
        private FinalizerPhantomReference(T referent, ReferenceQueue<? super T> queue, Callable<?> cleanup)
        {
            super(referent, queue);
            this.cleanup = cleanup;
        }

        private void cleanup()
                throws Exception
        {
            // CAS原子操作
            if (cleaned.compareAndSet(false, true)) {
                cleanup.call();
            }
        }
    }

    private class FinalizerQueueProcessor
            implements Runnable
    {
        @Override
        public void run()
        {
            while (!destroyed.get()) {
                // 清除引用队列中的引用对象
                FinalizerPhantomReference<T> reference;
                try {
                    reference = (FinalizerPhantomReference<T>) referenceQueue.remove();
                }
                catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }

                // 在ConcurrentHashMap中移除该虚引用对象
                references.remove(reference);

                boolean rescheduleAndReturn = false;
                try {
                    reference.cleanup();
                    rescheduleAndReturn = Thread.currentThread().isInterrupted();
                }
                catch (Throwable userException) {
                    try {
                        monitor.unexpectedException(userException);
                    }
                    catch (Exception ignored) {
                        // todo consider a broader notification
                    }

                    if (userException instanceof InterruptedException) {
                        rescheduleAndReturn = true;
                        Thread.currentThread().interrupt();
                    }
                    else if (userException instanceof Error) {
                        rescheduleAndReturn = true;
                    }
                }

                if (rescheduleAndReturn) {
                    synchronized (Finalizer.this) {
                        if (!destroyed.get()) {
                            executor.submit(new FinalizerQueueProcessor());
                        }
                    }
                    return;
                }
            }
        }
    }
}
