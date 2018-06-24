/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.zookeeper.server;

import org.apache.zookeeper.common.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * WorkerService 是worker线程池,用于执行任务.其使用一个或多个ExecutorService实现.
 * 可指定线程模式:则生成N个ExecutorService,每个ExecutorService只包含一个线程
 * 不可指定线程模式:则生成1个ExecutorService,其中有N个线程
 * - NIOServerCnxnFactory使用不可指定线程模式的WorkerService,因为网络IO请求无需有序执行,
 * 让ExecutorService处理线程的分配可以获得最佳的性能.
 * - CommitProcessor使用可指定线程模式的WorkerService,因为一个会话的多个请求必须顺序执行.
 * ExecutorService提供队列管理和线程重启,所有即使使用只有一个线程的ExecutorService
 * 也比直接使用Thread更方便
 * <p>
 * <p>
 * WorkerService is a worker thread pool for running tasks and is implemented
 * using one or more ExecutorServices. A WorkerService can support assignable
 * threads, which it does by creating N separate single thread ExecutorServices,
 * or non-assignable threads, which it does by creating a single N-thread
 * ExecutorService.
 * - NIOServerCnxnFactory uses a non-assignable WorkerService because the
 * socket IO requests are order independent and allowing the
 * ExecutorService to handle thread assignment gives optimal performance.
 * - CommitProcessor uses an assignable WorkerService because requests for
 * a given session must be processed in order.
 * ExecutorService provides queue management and thread restarting, so it's
 * useful even with a single thread.
 */
public class WorkerService {
    private static final Logger LOG =
            LoggerFactory.getLogger(WorkerService.class);

    /**
     * 线程池队列
     */
    private final List<ExecutorService> workers =
            new ArrayList<>();

    /**
     * 线程名前缀
     */
    private final String threadNamePrefix;
    /**
     * worker thread的个数
     */
    private int numWorkerThreads;
    /**
     * worker是线程池列表,有两种分配线程的方式
     * 1.列表中只有一个线程池,但线程池大小为numWorkerThreads
     * 2.列表中有numWorkerThreads个线程池,但每个线程池只有一个线程
     * 若此值为true,则使用第二种方式
     */
    private boolean threadsAreAssignable;
    private long shutdownTimeoutMS = 5000;

    private volatile boolean stopped = true;

    /**
     * @param name                 worker threads are named <name>Thread-##
     * @param numThreads           number of worker threads (0 - N)
     *                             If 0, scheduled work is run immediately by
     *                             the calling thread.
     * @param useAssignableThreads whether the worker threads should be
     *                             individually assignable or not
     */
    public WorkerService(String name, int numThreads,
                         boolean useAssignableThreads) {
        this.threadNamePrefix = (name == null ? "" : name) + "Thread";
        this.numWorkerThreads = numThreads;
        this.threadsAreAssignable = useAssignableThreads;
        start();
    }

    /**
     * Callers should implement a class extending WorkRequest in order to
     * schedule work with the service.
     */
    public static abstract class WorkRequest {
        /**
         * Must be implemented. Is called when the work request is run.
         */
        public abstract void doWork() throws Exception;

        /**
         * (Optional) If implemented, is called if the service is stopped
         * or unable to schedule the request.
         */
        public void cleanup() {
        }
    }

    /**
     * 处理workRequest,若worker thread个数为0,则在主线程中完成处理.
     * 此方法永远将workRequest交由worker thread中的第一个线程
     * <p>
     * Schedule work to be done.  If a worker thread pool is not being
     * used, work is done directly by this thread. This schedule API is
     * for use with non-assignable WorkerServices. For assignable
     * WorkerServices, will always run on the first thread.
     *
     * @param workRequest 待处理的IO请求
     */
    public void schedule(WorkRequest workRequest) {
        schedule(workRequest, 0);
    }

    /**
     * Schedule work to be done by the thread assigned to this id. Thread
     * assignment is a single mod operation on the number of threads.  If a
     * worker thread pool is not being used, work is done directly by
     * this thread.
     * 根据id取模将workRequest分配给对应的线程.如果没有使用worker thread
     * (即numWorkerThreads=0),则启动ScheduledWorkRequest线程完成任务,当前
     * 线程阻塞到任务完成.
     *
     * @param workRequest 待处理的IO请求
     * @param id          根据此值选择使用哪一个thread处理workRequest
     */
    public void schedule(WorkRequest workRequest, long id) {
        if (stopped) {
            workRequest.cleanup();
            return;
        }

        ScheduledWorkRequest scheduledWorkRequest =
                new ScheduledWorkRequest(workRequest);

        // If we have a worker thread pool, use that;
        // otherwise, do the work directly.
        int size = workers.size();
        if (size > 0) {
            try {
                // make sure to map negative ids as well to [0, size-1]
                int workerNum = ((int) (id % size) + size) % size;
                ExecutorService worker = workers.get(workerNum);
                worker.execute(scheduledWorkRequest);
            } catch (RejectedExecutionException e) {
                LOG.warn("ExecutorService rejected execution", e);
                workRequest.cleanup();
            }
        } else {
            // When there is no worker thread pool, do the work directly
            // and wait for its completion
            scheduledWorkRequest.start();
            try {
                scheduledWorkRequest.join();
            } catch (InterruptedException e) {
                LOG.warn("Unexpected exception", e);
                Thread.currentThread().interrupt();
            }
        }
    }

    /**
     * 若worker thread个数不为0,则该类仅作为Runnable交由{@link #workers}执行;
     * 若worker thread个数为0,则该类当做线程被启动,执行WorkRequest,在主线程中调用ScheduledWorkRequest.join()等待该线程执行完毕.
     * 个人认为:若worker thread个数为0,每个WorkerRequest都需要启动ScheduledWorkRequest线程进行处理,而且主线程还要阻塞到该线程执行完毕,
     * 对性能的损耗简直不能忍,因此在实际生产中,不能将worker thread个数设置为0
     */
    private class ScheduledWorkRequest extends ZooKeeperThread {
        private final WorkRequest workRequest;

        ScheduledWorkRequest(WorkRequest workRequest) {
            super("ScheduledWorkRequest");
            this.workRequest = workRequest;
        }

        @Override
        public void run() {
            try {
                // Check if stopped while request was on queue
                if (stopped) {
                    workRequest.cleanup();
                    return;
                }
                workRequest.doWork();
            } catch (Exception e) {
                LOG.warn("Unexpected exception", e);
                workRequest.cleanup();
            }
        }
    }

    /**
     * ThreadFactory for the worker thread pool. We don't use the default
     * thread factory because (1) we want to give the worker threads easier
     * to identify names; and (2) we want to make the worker threads daemon
     * threads so they don't block the server from shutting down.
     */
    private static class DaemonThreadFactory implements ThreadFactory {
        final ThreadGroup group;
        final AtomicInteger threadNumber = new AtomicInteger(1);
        final String namePrefix;

        DaemonThreadFactory(String name) {
            this(name, 1);
        }

        DaemonThreadFactory(String name, int firstThreadNum) {
            threadNumber.set(firstThreadNum);
            SecurityManager s = System.getSecurityManager();
            group = (s != null) ? s.getThreadGroup() :
                    Thread.currentThread().getThreadGroup();
            namePrefix = name + "-";
        }

        @Override
        public Thread newThread(Runnable r) {
            Thread t = new Thread(group, r,
                    namePrefix + threadNumber.getAndIncrement(),
                    0);
            if (!t.isDaemon()) {
                t.setDaemon(true);
            }
            if (t.getPriority() != Thread.NORM_PRIORITY) {
                t.setPriority(Thread.NORM_PRIORITY);
            }
            return t;
        }
    }

    /**
     * 初始化{@link #workers}
     */
    public void start() {
        if (numWorkerThreads > 0) {
            if (threadsAreAssignable) {
                for (int i = 1; i <= numWorkerThreads; ++i) {
                    workers.add(Executors.newFixedThreadPool(
                            1, new DaemonThreadFactory(threadNamePrefix, i)));
                }
            } else {
                workers.add(Executors.newFixedThreadPool(
                        numWorkerThreads, new DaemonThreadFactory(threadNamePrefix)));
            }
        }
        stopped = false;
    }

    public void stop() {
        stopped = true;

        // Signal for graceful shutdown
        for (ExecutorService worker : workers) {
            worker.shutdown();
        }
    }

    public void join(long shutdownTimeoutMS) {
        // Give the worker threads time to finish executing
        long now = Time.currentElapsedTime();
        long endTime = now + shutdownTimeoutMS;
        for (ExecutorService worker : workers) {
            boolean terminated = false;
            while ((now = Time.currentElapsedTime()) <= endTime) {
                try {
                    terminated = worker.awaitTermination(
                            endTime - now, TimeUnit.MILLISECONDS);
                    break;
                } catch (InterruptedException e) {
                    // ignore
                }
            }
            if (!terminated) {
                // If we've timed out, do a hard shutdown
                worker.shutdownNow();
            }
        }
    }
}
