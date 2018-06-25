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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * NIOServerCnxnFactory implements a multi-threaded ServerCnxnFactory using
 * NIO non-blocking socket calls. Communication between threads is handled via
 * queues.
 * NIO实现了一个多线程的ServerCnxnFactory，线程间的通信都是通过queue来完成的
 * <p>
 * - 1   accept thread, which accepts new connections and assigns to a
 * selector thread
 * - 1-N selector threads, each of which selects on 1/N of the connections.
 * The reason the factory supports more than one selector thread is that
 * with large numbers of connections, select() itself can become a
 * performance bottleneck.
 * - 0-M socket I/O worker threads, which perform basic socket reads and
 * writes. If configured with 0 worker threads, the selector threads
 * do the socket I/O directly.
 * - 1   connection expiration thread, which closes idle connections; this is
 * necessary to expire connections on which no session is established.
 * <p>
 * Typical (default) thread counts are: on a 32 core machine, 1 accept thread,
 * 1 connection expiration thread, 4 selector threads, and 64 worker threads.
 * <p>
 */
public class NIOServerCnxnFactory extends ServerCnxnFactory {
    private static final Logger LOG = LoggerFactory.getLogger(NIOServerCnxnFactory.class);

    /**
     * Default sessionless connection timeout in ms: 10000 (10s)
     */
    public static final String ZOOKEEPER_NIO_SESSIONLESS_CNXN_TIMEOUT =
            "zookeeper.nio.sessionlessCnxnTimeout";
    /**
     * With 500 connections to an observer with watchers firing on each, is
     * unable to exceed 1GigE rates with only 1 selector.
     * Defaults to using 2 selector threads with 8 cores and 4 with 32 cores.
     * Expressed as sqrt(numCores/2). Must have at least 1 selector thread.
     */
    public static final String ZOOKEEPER_NIO_NUM_SELECTOR_THREADS =
            "zookeeper.nio.numSelectorThreads";
    /**
     * Default: 2 * numCores
     */
    public static final String ZOOKEEPER_NIO_NUM_WORKER_THREADS =
            "zookeeper.nio.numWorkerThreads";
    /**
     * Default: 64kB
     */
    public static final String ZOOKEEPER_NIO_DIRECT_BUFFER_BYTES =
            "zookeeper.nio.directBufferBytes";
    /**
     * Default worker pool shutdown timeout in ms: 5000 (5s)
     */
    public static final String ZOOKEEPER_NIO_SHUTDOWN_TIMEOUT =
            "zookeeper.nio.shutdownTimeout";

    static {
        Thread.setDefaultUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler() {
            @Override
            public void uncaughtException(Thread t, Throwable e) {
                LOG.error("Thread " + t + " died", e);
            }
        });
        /**
         * this is to avoid the jvm bug:
         * NullPointerException in Selector.open()
         * http://bugs.sun.com/view_bug.do?bug_id=6427854
         */
        try {
            Selector.open().close();
        } catch (IOException ie) {
            LOG.error("Selector failed to open", ie);
        }

        /**
         * Value of 0 disables use of direct buffers and instead uses
         * gathered write call.
         *
         * Default to using 64k direct buffers.
         */
        directBufferBytes = Integer.getInteger(
                ZOOKEEPER_NIO_DIRECT_BUFFER_BYTES, 64 * 1024);
    }

    /**
     * AbstractSelectThread is an abstract base class containing a few bits
     * of code shared by the AcceptThread (which selects on the listen socket)
     * and SelectorThread (which selects on client connections) classes.
     */
    private abstract class AbstractSelectThread extends ZooKeeperThread {
        protected final Selector selector;

        public AbstractSelectThread(String name) throws IOException {
            super(name);
            // Allows the JVM to shutdown even if this thread is still running.
            setDaemon(true);
            this.selector = Selector.open();
        }

        public void wakeupSelector() {
            selector.wakeup();
        }

        /**
         * Close the selector. This should be called when the thread is about to
         * exit and no operation is going to be performed on the Selector or
         * SelectionKey
         */
        protected void closeSelector() {
            try {
                selector.close();
            } catch (IOException e) {
                LOG.warn("ignored exception during selector close "
                        + e.getMessage());
            }
        }

        protected void cleanupSelectionKey(SelectionKey key) {
            if (key != null) {
                try {
                    key.cancel();
                } catch (Exception ex) {
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("ignoring exception during selectionkey cancel", ex);
                    }
                }
            }
        }

        protected void fastCloseSock(SocketChannel sc) {
            if (sc != null) {
                try {
                    // Hard close immediately, discarding buffers
                    sc.socket().setSoLinger(true, 0);
                } catch (SocketException e) {
                    LOG.warn("Unable to set socket linger to 0, socket close"
                            + " may stall in CLOSE_WAIT", e);
                }
                NIOServerCnxn.closeSock(sc);
            }
        }
    }

    /**
     * There is a single AcceptThread which accepts new connections and assigns
     * them to a SelectorThread using a simple round-robin scheme to spread
     * them across the SelectorThreads. It enforces maximum number of
     * connections per IP and attempts to cope with running out of file
     * descriptors by briefly sleeping before retrying.
     * <p>
     * accept线程主要监听连接事件，建立连接后分派给selector。在退出时，关闭它自身的selector，然后唤醒用来进行socket I/O的selector线程，最后通知worker线程退出。
     */
    private class AcceptThread extends AbstractSelectThread {
        private final ServerSocketChannel acceptSocket;
        /**
         * acceptSocket注册到selector时返回的selectionKey
         */
        private final SelectionKey acceptKey;
        private final RateLogger acceptErrorLogger = new RateLogger(LOG);
        private final Collection<SelectorThread> selectorThreads;
        private Iterator<SelectorThread> selectorIterator;
        private volatile boolean reconfiguring = false;

        public AcceptThread(ServerSocketChannel ss, InetSocketAddress addr,
                            Set<SelectorThread> selectorThreads) throws IOException {
            super("NIOServerCxnFactory.AcceptThread:" + addr);
            this.acceptSocket = ss;
            //将ServerSocketChannel注册到selector上,监听OP_ACCEPT事件
            this.acceptKey =
                    acceptSocket.register(selector, SelectionKey.OP_ACCEPT);
            this.selectorThreads = Collections.unmodifiableList(
                    new ArrayList<>(selectorThreads));
            selectorIterator = this.selectorThreads.iterator();
        }

        @Override
        public void run() {
            try {
                while (!stopped && !acceptSocket.socket().isClosed()) {
                    try {
                        //监听连接事件,并建立连接
                        select();
                    } catch (RuntimeException e) {
                        LOG.warn("Ignoring unexpected runtime exception", e);
                    } catch (Exception e) {
                        LOG.warn("Ignoring unexpected exception", e);
                    }
                }
            } finally {
                //关闭selector
                closeSelector();
                // This will wake up the selector threads, and tell the
                // worker thread pool to begin shutdown.
                if (!reconfiguring) {
                    //唤醒selector线程并通知worker线程关闭
                    NIOServerCnxnFactory.this.stop();
                }
                LOG.info("accept thread exitted run method");
            }
        }

        public void setReconfiguring() {
            reconfiguring = true;
        }

        private void select() {
            try {
                selector.select();

                Iterator<SelectionKey> selectedKeys =
                        selector.selectedKeys().iterator();
                while (!stopped && selectedKeys.hasNext()) {
                    SelectionKey key = selectedKeys.next();
                    //selector.selectedKeys()返回的永远是同一个集合类,
                    // 因此若不主动删除已处理的selectedKey,该selectedKey会一直存在在集合中,
                    //下次selector.selectedKeys()还会返回该selectedKey,而此时并没有客户端连接到来
                    selectedKeys.remove();

                    if (!key.isValid()) {
                        continue;
                    }
                    if (key.isAcceptable()) {
                        //ACCEPT事件
                        if (!doAccept()) {
                            // If unable to pull a new connection off the accept
                            // queue, pause accepting to give us time to free
                            // up file descriptors and so the accept thread
                            // doesn't spin in a tight loop.
                            pauseAccept(10);
                        }
                    } else {
                        LOG.warn("Unexpected ops in accept select "
                                + key.readyOps());
                    }
                }
            } catch (IOException e) {
                LOG.warn("Ignoring IOException while selecting", e);
            }
        }

        /**
         * Mask off the listen socket interest ops and use select() to sleep
         * so that other threads can wake us up by calling wakeup() on the
         * selector.
         */
        private void pauseAccept(long millisecs) {
            acceptKey.interestOps(0);
            try {
                selector.select(millisecs);
            } catch (IOException e) {
                // ignore
            } finally {
                acceptKey.interestOps(SelectionKey.OP_ACCEPT);
            }
        }

        /**
         * 接收新的socket连接,每个IP地址有其连接个数上限.使用轮询为新连接分配selector thread
         * <p>
         * Accept new socket connections. Enforces(强制实施) maximum number of connections
         * per client IP address. Round-robin assigns to selector thread for
         * handling. Returns whether pulled a connection off the accept queue
         * or not. If encounters an error attempts to fast close the socket.
         *
         * @return whether was able to accept a connection or not
         */
        private boolean doAccept() {
            boolean accepted = false;
            SocketChannel sc = null;
            try {
                sc = acceptSocket.accept();
                accepted = true;
                //防止来自一个IP地址的连接过多
                InetAddress ia = sc.socket().getInetAddress();
                int cnxncount = getClientCnxnCount(ia);
                //判断是否最大客户端连接的限制
                if (maxClientCnxns > 0 && cnxncount >= maxClientCnxns) {
                    throw new IOException("Too many connections from " + ia
                            + " - max is " + maxClientCnxns);
                }

                LOG.info("Accepted socket connection from "
                        + sc.socket().getRemoteSocketAddress());
                sc.configureBlocking(false);

                // Round-robin assign this connection to a selector thread
                //使用轮询将连接分派给某个selector线程
                if (!selectorIterator.hasNext()) {
                    selectorIterator = selectorThreads.iterator();
                }
                //将新连接加入selector thread 的acceptedQueue中
                SelectorThread selectorThread = selectorIterator.next();
                if (!selectorThread.addAcceptedConnection(sc)) {
                    throw new IOException(
                            "Unable to add connection to selector queue"
                                    + (stopped ? " (shutdown in progress)" : ""));
                }
                acceptErrorLogger.flush();
            } catch (IOException e) {
                // accept, maxClientCnxns, configureBlocking
                acceptErrorLogger.rateLimitLog(
                        "Error accepting new connection: " + e.getMessage());
                fastCloseSock(sc);
            }
            return accepted;
        }
    }

    /**
     * The SelectorThread receives newly accepted connections from the
     * AcceptThread and is responsible for selecting for I/O readiness
     * across the connections. This thread is the only thread that performs
     * any non-threadsafe or potentially blocking calls on the selector
     * (registering new connections and reading/writing interest ops).
     * <p>
     * Assignment of a connection to a SelectorThread is permanent and only
     * one SelectorThread will ever interact with the connection. There are
     * 1-N SelectorThreads, with connections evenly apportioned between the
     * SelectorThreads.
     * <p>
     * If there is a worker thread pool, when a connection has I/O to perform
     * the SelectorThread removes it from selection by clearing its interest
     * ops and schedules the I/O for processing by a worker thread. When the
     * work is complete, the connection is placed on the ready queue to have
     * its interest ops restored and resume selection.
     * <p>
     * If there is no worker thread pool, the SelectorThread performs the I/O
     * directly.
     */
    class SelectorThread extends AbstractSelectThread {
        private final int id;
        /**
         * accept thread分配给该线程的连接,其中的连接尚未注册到selector上
         */
        private final Queue<SocketChannel> acceptedQueue;
        /**
         * 由于NIO是水平触发,在将IO事件交由{@link #workerPool}处理之前,调用key.interestOps(0)表示连接不再监听任何事件;
         * 因此在{@link #workerPool}将IO事件处理结束之后,连接还需监听OP_READ和OP_WRITE.selector thread和worker thread使用
         * 该队列作为通信工具,worker thread将IO事件处理结束后,需要监听OP_READ和OP_WRITE的连接放入此队列
         */
        private final Queue<SelectionKey> updateQueue;

        public SelectorThread(int id) throws IOException {
            super("NIOServerCxnFactory.SelectorThread-" + id);
            this.id = id;
            acceptedQueue = new LinkedBlockingQueue<>();
            updateQueue = new LinkedBlockingQueue<>();
        }

        /**
         * Place new accepted connection onto a queue for adding. Do this
         * so only the selector thread modifies what keys are registered
         * with the selector.
         * 将一个已连接的connection放入{@link #acceptedQueue},
         * 这样可以确保只有{@link SelectorThread}可以修改socketChannel会监听哪些操作
         */
        public boolean addAcceptedConnection(SocketChannel accepted) {
            if (stopped || !acceptedQueue.offer(accepted)) {
                return false;
            }
            //唤醒selector进行处理
            wakeupSelector();
            return true;
        }

        /**
         * Place interest op update requests onto a queue so that only the
         * selector thread modifies interest ops, because interest ops
         * reads/sets are potentially blocking operations if other select
         * operations are happening.
         * <p>
         * 将更新interest op请求放到{@link #updateQueue}中,这样使得只有selector thread才可以修改interest ops,
         * 因为selector thread执行select()时,其他线程读取或设置interest ops可能会阻塞select()操作
         */
        public boolean addInterestOpsUpdateRequest(SelectionKey sk) {
            if (stopped || !updateQueue.offer(sk)) {
                return false;
            }
            wakeupSelector();
            return true;
        }

        /**
         * The main loop for the thread selects() on the connections and
         * dispatches ready I/O work requests, then registers all pending
         * newly accepted connections and updates any interest ops on the
         * queue.
         */
        @Override
        public void run() {
            try {
                while (!stopped) {
                    try {
                        //1.调用select()读取就绪的IO事件,交由worker thread处理
                        select();
                        //2.处理accept线程新分派的连接,
                        // (1)将新连接注册到selector上;(2)包装为NIOServerCnxn后注册到NIOServerCnxnFactory中
                        processAcceptedConnections();
                        //3.更新updateQueue中连接的监听事件
                        processInterestOpsUpdateRequests();
                    } catch (RuntimeException e) {
                        LOG.warn("Ignoring unexpected runtime exception", e);
                    } catch (Exception e) {
                        LOG.warn("Ignoring unexpected exception", e);
                    }
                }

                // Close connections still pending on the selector. Any others
                // with in-flight work, let drain out of the work queue.
                //执行清理操作,关闭所有在selector上等待的连接
                for (SelectionKey key : selector.keys()) {
                    NIOServerCnxn cnxn = (NIOServerCnxn) key.attachment();
                    if (cnxn.isSelectable()) {
                        cnxn.close();
                    }
                    cleanupSelectionKey(key);
                }
                SocketChannel accepted;
                while ((accepted = acceptedQueue.poll()) != null) {
                    fastCloseSock(accepted);
                }
                updateQueue.clear();
            } finally {
                closeSelector();
                // This will wake up the accept thread and the other selector
                // threads, and tell the worker thread pool to begin shutdown.
                // 唤醒accept线程及其他线程，并通知worker线程退出
                NIOServerCnxnFactory.this.stop();
                LOG.info("selector thread exitted run method");
            }
        }

        private void select() {
            try {
                selector.select();

                Set<SelectionKey> selected = selector.selectedKeys();
                List<SelectionKey> selectedList =
                        new ArrayList<>(selected);
                Collections.shuffle(selectedList);
                Iterator<SelectionKey> selectedKeys = selectedList.iterator();
                while (!stopped && selectedKeys.hasNext()) {
                    SelectionKey key = selectedKeys.next();
                    //此处依然需要remove
                    selected.remove(key);

                    if (!key.isValid()) {
                        cleanupSelectionKey(key);
                        continue;
                    }
                    if (key.isReadable() || key.isWritable()) {
                        //IO事件
                        handleIO(key);
                    } else {
                        LOG.warn("Unexpected ops in select " + key.readyOps());
                    }
                }
            } catch (IOException e) {
                LOG.warn("Ignoring IOException while selecting", e);
            }
        }

        /**
         * Schedule I/O for processing on the connection associated with
         * the given SelectionKey. If a worker thread pool is not being used,
         * I/O is run directly by this thread.
         */
        private void handleIO(SelectionKey key) {
            //把有事件发生的连接封装成IOWorkRequest对象，
            IOWorkRequest workRequest = new IOWorkRequest(this, key);
            NIOServerCnxn cnxn = (NIOServerCnxn) key.attachment();

            // Stop selecting this key while processing on its
            // connection
            //由于selector.select()是水平触发,因此在WorkThread完成此次IO处理之前,此连接不应被select()
            cnxn.disableSelectable();
            key.interestOps(0);
            touchCnxn(cnxn);
            //将workRequest交给worker线程来处理
            workerPool.schedule(workRequest);
        }

        /**
         * 处理accept thread 分配的但是尚未注册到selector上的socketchannel
         * <p>
         * Iterate over the queue of accepted connections that have been
         * assigned to this thread but not yet placed on the selector.
         */
        private void processAcceptedConnections() {
            SocketChannel accepted;
            while (!stopped && (accepted = acceptedQueue.poll()) != null) {
                SelectionKey key = null;
                try {
                    key = accepted.register(selector, SelectionKey.OP_READ);
                    //生成NIOServerCnxn
                    NIOServerCnxn cnxn = createConnection(accepted, key, this);
                    //将NIOServerCnxn设置为key的attachment
                    key.attach(cnxn);
                    //将其添加到NIOServerCnxnFactory中
                    addCnxn(cnxn);
                } catch (IOException e) {
                    // register, createConnection
                    cleanupSelectionKey(key);
                    fastCloseSock(accepted);
                }
            }
        }

        /**
         * Iterate over the queue of connections ready to resume selection,
         * and restore their interest ops selection mask.
         */
        private void processInterestOpsUpdateRequests() {
            SelectionKey key;
            while (!stopped && (key = updateQueue.poll()) != null) {
                if (!key.isValid()) {
                    cleanupSelectionKey(key);
                }
                NIOServerCnxn cnxn = (NIOServerCnxn) key.attachment();
                if (cnxn.isSelectable()) {
                    key.interestOps(cnxn.getInterestOps());
                }
            }
        }
    }

    /**
     * IOWorkRequest is a small wrapper class to allow doIO() calls to be
     * run on a connection using a WorkerService.
     */
    private class IOWorkRequest extends WorkerService.WorkRequest {
        private final SelectorThread selectorThread;
        private final SelectionKey key;
        private final NIOServerCnxn cnxn;

        IOWorkRequest(SelectorThread selectorThread, SelectionKey key) {
            this.selectorThread = selectorThread;
            this.key = key;
            this.cnxn = (NIOServerCnxn) key.attachment();
        }

        @Override
        public void doWork() throws InterruptedException {
            //如果Channel已经关闭则清理该SelectionKey
            if (!key.isValid()) {
                selectorThread.cleanupSelectionKey(key);
                return;
            }
            //如果可读或可写，则调用NIOServerCnxn.doIO()方法，通知NIOServerCnxn连接对象进行IO读写及处理
            if (key.isReadable() || key.isWritable()) {
                cnxn.doIO(key);

                // Check if we shutdown or doIO() closed this connection
                //如果已经shutdown则关闭连接
                if (stopped) {
                    cnxn.close();
                    return;
                }
                //如果Channel已经关闭则清理该SelectionKey
                if (!key.isValid()) {
                    selectorThread.cleanupSelectionKey(key);
                    return;
                }
                //更新该连接的过期时间
                touchCnxn(cnxn);
            }

            // Mark this connection as once again ready for selection
            //已经处理完读写，重新标记该连接已准备好新的select事件监听
            cnxn.enableSelectable();
            // Push an update request on the queue to resume selecting
            // on the current set of interest ops, which may have changed
            // as a result of the I/O operations we just performed.
            //把该连接重新放到selectThread的updateQueue中，selectThread会在处理处理完所有Channel的读写和新连接后，更新此Channel的注册监听事件
            if (!selectorThread.addInterestOpsUpdateRequest(key)) {
                cnxn.close();
            }
        }

        @Override
        public void cleanup() {
            cnxn.close();
        }
    }

    /**
     * 此线程用于关闭过期的连接,这样没有会话的连接就会过期.
     * <p>
     * This thread is responsible for closing stale connections so that
     * connections on which no session is established are properly expired.
     */
    private class ConnectionExpirerThread extends ZooKeeperThread {
        ConnectionExpirerThread() {
            super("ConnnectionExpirer");
        }

        @Override
        public void run() {
            try {
                while (!stopped) {
                    long waitTime = cnxnExpiryQueue.getWaitTime();
                    if (waitTime > 0) {
                        Thread.sleep(waitTime);
                        continue;
                    }
                    for (NIOServerCnxn conn : cnxnExpiryQueue.poll()) {
                        conn.close();
                    }
                }

            } catch (InterruptedException e) {
                LOG.info("ConnnectionExpirerThread interrupted");
            }
        }
    }

    ServerSocketChannel ss;

    /**
     * 使用其执行高效的socket I/O,由于I/O由worker thread执行,因此将直接内存设置为ThreadLocal的.
     * 各连接可以在共享直接内存的同时无需担心并发问题.
     * <p>
     * We use this buffer to do efficient socket I/O. Because I/O is handled
     * by the worker threads (or the selector threads directly, if no worker
     * thread pool is created), we can create a fixed set of these to be
     * shared by connections.
     */
    private static final ThreadLocal<ByteBuffer> directBuffer =
            new ThreadLocal<ByteBuffer>() {
                @Override
                protected ByteBuffer initialValue() {
                    return ByteBuffer.allocateDirect(directBufferBytes);
                }
            };

    public static ByteBuffer getDirectBuffer() {
        return directBufferBytes > 0 ? directBuffer.get() : null;
    }

    /**
     * sessionMap is used by closeSession()
     * key:sessionId
     * value:客户端连接
     */
    private final ConcurrentHashMap<Long, NIOServerCnxn> sessionMap =
            new ConcurrentHashMap<>();
    /**
     * ipMap is used to limit connections per IP
     * key:客户端ip地址
     * value:该ip建立的连接
     */
    private final ConcurrentHashMap<InetAddress, Set<NIOServerCnxn>> ipMap =
            new ConcurrentHashMap<>();

    protected int maxClientCnxns = 60;

    int sessionlessCnxnTimeout;
    private ExpiryQueue<NIOServerCnxn> cnxnExpiryQueue;


    protected WorkerService workerPool;

    /**
     * 发送数据时使用的直接内存字节数
     */
    private static int directBufferBytes;
    /**
     * selector thread的个数
     */
    private int numSelectorThreads;
    /**
     * worker thread的个数
     */
    private int numWorkerThreads;
    private long workerShutdownTimeoutMS;

    /**
     * Construct a new server connection factory which will accept an unlimited number
     * of concurrent connections from each client (up to the file descriptor
     * limits of the operating system). startup(zks) must be called subsequently.
     */
    public NIOServerCnxnFactory() {
    }

    private volatile boolean stopped = true;
    private ConnectionExpirerThread expirerThread;
    private AcceptThread acceptThread;
    private final Set<SelectorThread> selectorThreads =
            new HashSet<>();

    @Override
    public void configure(InetSocketAddress addr, int maxcc, boolean secure) throws IOException {
        if (secure) {
            throw new UnsupportedOperationException("SSL isn't supported in NIOServerCnxn");
        }
        configureSaslLogin();

        maxClientCnxns = maxcc;
        sessionlessCnxnTimeout = Integer.getInteger(
                ZOOKEEPER_NIO_SESSIONLESS_CNXN_TIMEOUT, 10000);
        // We also use the sessionlessCnxnTimeout as expiring interval for
        // cnxnExpiryQueue. These don't need to be the same, but the expiring
        // interval passed into the ExpiryQueue() constructor below should be
        // less than or equal to the timeout.
        cnxnExpiryQueue =
                new ExpiryQueue<>(sessionlessCnxnTimeout);
        expirerThread = new ConnectionExpirerThread();

        int numCores = Runtime.getRuntime().availableProcessors();
        // 32 cores sweet spot seems to be 4 selector threads
        numSelectorThreads = Integer.getInteger(
                ZOOKEEPER_NIO_NUM_SELECTOR_THREADS,
                Math.max((int) Math.sqrt((float) numCores / 2), 1));
        if (numSelectorThreads < 1) {
            throw new IOException("numSelectorThreads must be at least 1");
        }

        numWorkerThreads = Integer.getInteger(
                ZOOKEEPER_NIO_NUM_WORKER_THREADS, 2 * numCores);
        workerShutdownTimeoutMS = Long.getLong(
                ZOOKEEPER_NIO_SHUTDOWN_TIMEOUT, 5000);

        LOG.info("Configuring NIO connection handler with "
                + (sessionlessCnxnTimeout / 1000) + "s sessionless connection"
                + " timeout, " + numSelectorThreads + " selector thread(s), "
                + (numWorkerThreads > 0 ? numWorkerThreads : "no")
                + " worker threads, and "
                + (directBufferBytes == 0 ? "gathered writes." :
                ("" + (directBufferBytes / 1024) + " kB direct buffers.")));
        //初始化ServerCnxnFactory的主线程
        for (int i = 0; i < numSelectorThreads; ++i) {
            selectorThreads.add(new SelectorThread(i));
        }
        //初始化NIO服务器
        this.ss = ServerSocketChannel.open();
        ss.socket().setReuseAddress(true);
        LOG.info("binding to port " + addr);
        ss.socket().bind(addr);
        ss.configureBlocking(false);
        //初始化acceptThread
        acceptThread = new AcceptThread(ss, addr, selectorThreads);
    }

    private void tryClose(ServerSocketChannel s) {
        try {
            s.close();
        } catch (IOException sse) {
            LOG.error("Error while closing server socket.", sse);
        }
    }

    @Override
    public void reconfigure(InetSocketAddress addr) {
        ServerSocketChannel oldSS = ss;
        try {
            this.ss = ServerSocketChannel.open();
            ss.socket().setReuseAddress(true);
            LOG.info("binding to port " + addr);
            ss.socket().bind(addr);
            ss.configureBlocking(false);
            acceptThread.setReconfiguring();
            tryClose(oldSS);
            acceptThread.wakeupSelector();
            try {
                acceptThread.join();
            } catch (InterruptedException e) {
                LOG.error("Error joining old acceptThread when reconfiguring client port {}",
                        e.getMessage());
                Thread.currentThread().interrupt();
            }
            acceptThread = new AcceptThread(ss, addr, selectorThreads);
            acceptThread.start();
        } catch (IOException e) {
            LOG.error("Error reconfiguring client port to {} {}", addr, e.getMessage());
            tryClose(oldSS);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int getMaxClientCnxnsPerHost() {
        return maxClientCnxns;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setMaxClientCnxnsPerHost(int max) {
        maxClientCnxns = max;
    }

    @Override
    public void start() {
        stopped = false;
        //worker线程,进行socket的读写
        if (workerPool == null) {
            workerPool = new WorkerService(
                    "NIOWorker", numWorkerThreads, false);
        }
        //selector线程,监听socket事件
        for (SelectorThread thread : selectorThreads) {
            if (thread.getState() == Thread.State.NEW) {
                thread.start();
            }
        }
        // ensure thread is started once and only once
        //accept线程
        if (acceptThread.getState() == Thread.State.NEW) {
            acceptThread.start();
        }
        //连接管理线程
        if (expirerThread.getState() == Thread.State.NEW) {
            expirerThread.start();
        }
    }

    @Override
    public void startup(ZooKeeperServer zks, boolean startServer)
            throws IOException, InterruptedException {
        //启动各线程
        start();
        setZooKeeperServer(zks);
        //恢复本地数据
        if (startServer) {
            zks.startdata();
            zks.startup();
        }
    }

    @Override
    public InetSocketAddress getLocalAddress() {
        return (InetSocketAddress) ss.socket().getLocalSocketAddress();
    }

    @Override
    public int getLocalPort() {
        return ss.socket().getLocalPort();
    }

    /**
     * De-registers the connection from the various mappings maintained
     * by the factory.
     * <p>
     * 取消NIOServerCnxn在NIOServerCnxnFactory上的注册
     */
    public boolean removeCnxn(NIOServerCnxn cnxn) {
        // If the connection is not in the master list it's already been closed
        if (!cnxns.remove(cnxn)) {
            return false;
        }
        cnxnExpiryQueue.remove(cnxn);

        long sessionId = cnxn.getSessionId();
        if (sessionId != 0) {
            sessionMap.remove(sessionId);
        }

        InetAddress addr = cnxn.getSocketAddress();
        if (addr != null) {
            Set<NIOServerCnxn> set = ipMap.get(addr);
            if (set != null) {
                set.remove(cnxn);
                // Note that we make no effort here to remove empty mappings
                // from ipMap.
            }
        }

        // unregister from JMX
        unregisterConnection(cnxn);
        return true;
    }

    /**
     * Add or update cnxn in our {@link #cnxnExpiryQueue}
     *
     * @param cnxn
     */
    public void touchCnxn(NIOServerCnxn cnxn) {
        cnxnExpiryQueue.update(cnxn, cnxn.getSessionTimeout());
    }

    /**
     * 将cnxn添加到NIOServerCnxnFactory的管理中
     *
     * @param cnxn 待处理的cnxn
     * @throws IOException
     */
    private void addCnxn(NIOServerCnxn cnxn) throws IOException {
        InetAddress addr = cnxn.getSocketAddress();
        if (addr == null) {
            throw new IOException("Socket of " + cnxn + " has been closed");
        }
        //将NIOServerCnxn添加至ipMap
        Set<NIOServerCnxn> set = ipMap.get(addr);
        if (set == null) {
            // in general we will see 1 connection from each
            // host, setting the initial cap to 2 allows us
            // to minimize mem usage in the common case
            // of 1 entry --  we need to set the initial cap
            // to 2 to avoid rehash when the first entry is added
            // Construct a ConcurrentHashSet using a ConcurrentHashMap
            set = Collections.newSetFromMap(
                    new ConcurrentHashMap<>(2));
            // Put the new set in the map, but only if another thread
            // hasn't beaten us to it
            Set<NIOServerCnxn> existingSet = ipMap.putIfAbsent(addr, set);
            if (existingSet != null) {
                set = existingSet;
            }
        }
        set.add(cnxn);
        //将NIOServerCnxn添加至集合
        cnxns.add(cnxn);
        //更新该NIOServerCnxn的过期时间
        touchCnxn(cnxn);
    }

    protected NIOServerCnxn createConnection(SocketChannel sock,
                                             SelectionKey sk, SelectorThread selectorThread) throws IOException {
        return new NIOServerCnxn(zkServer, sock, sk, this, selectorThread);
    }

    private int getClientCnxnCount(InetAddress cl) {
        Set<NIOServerCnxn> s = ipMap.get(cl);
        if (s == null) {
            return 0;
        }
        return s.size();
    }

    /**
     * clear all the connections in the selector
     */
    @Override
    @SuppressWarnings("unchecked")
    public void closeAll() {
        // clear all the connections on which we are selecting
        for (ServerCnxn cnxn : cnxns) {
            try {
                // This will remove the cnxn from cnxns
                cnxn.close();
            } catch (Exception e) {
                LOG.warn("Ignoring exception closing cnxn sessionid 0x"
                        + Long.toHexString(cnxn.getSessionId()), e);
            }
        }
    }

    public void stop() {
        stopped = true;

        // Stop queuing connection attempts
        try {
            ss.close();
        } catch (IOException e) {
            LOG.warn("Error closing listen socket", e);
        }

        if (acceptThread != null) {
            acceptThread.wakeupSelector();
        }
        if (expirerThread != null) {
            expirerThread.interrupt();
        }
        for (SelectorThread thread : selectorThreads) {
            thread.wakeupSelector();
        }
        if (workerPool != null) {
            workerPool.stop();
        }
    }

    public void shutdown() {
        try {
            // close listen socket and signal selector threads to stop
            stop();

            // wait for selector and worker threads to shutdown
            join();

            // close all open connections
            closeAll();

            if (login != null) {
                login.shutdown();
            }
        } catch (InterruptedException e) {
            LOG.warn("Ignoring interrupted exception during shutdown", e);
        } catch (Exception e) {
            LOG.warn("Ignoring unexpected exception during shutdown", e);
        }

        if (zkServer != null) {
            zkServer.shutdown();
        }
    }

    public void addSession(long sessionId, NIOServerCnxn cnxn) {
        sessionMap.put(sessionId, cnxn);
    }

    @Override
    public boolean closeSession(long sessionId) {
        NIOServerCnxn cnxn = sessionMap.remove(sessionId);
        if (cnxn != null) {
            cnxn.close();
            return true;
        }
        return false;
    }

    @Override
    public void join() throws InterruptedException {
        if (acceptThread != null) {
            acceptThread.join();
        }
        for (SelectorThread thread : selectorThreads) {
            thread.join();
        }
        if (workerPool != null) {
            workerPool.join(workerShutdownTimeoutMS);
        }
    }

    @Override
    public Iterable<ServerCnxn> getConnections() {
        return cnxns;
    }

    public void dumpConnections(PrintWriter pwriter) {
        pwriter.print("Connections ");
        cnxnExpiryQueue.dump(pwriter);
    }

    @Override
    public void resetAllConnectionStats() {
        // No need to synchronize since cnxns is backed by a ConcurrentHashMap
        for (ServerCnxn c : cnxns) {
            c.resetStats();
        }
    }

    @Override
    public Iterable<Map<String, Object>> getAllConnectionInfo(boolean brief) {
        HashSet<Map<String, Object>> info = new HashSet<Map<String, Object>>();
        // No need to synchronize since cnxns is backed by a ConcurrentHashMap
        for (ServerCnxn c : cnxns) {
            info.add(c.getConnectionInfo(brief));
        }
        return info;
    }
}
