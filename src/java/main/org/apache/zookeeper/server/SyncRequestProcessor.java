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

import java.io.Flushable;
import java.io.IOException;
import java.util.LinkedList;
import java.util.Random;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * This RequestProcessor logs requests to disk. It batches the requests to do
 * the io efficiently. The request is not passed to the next RequestProcessor
 * until its log has been synced to disk.
 * <p>
 * SyncRequestProcessor is used in 3 different cases
 * 1. Leader - Sync request to disk and forward it to AckRequestProcessor which
 * send ack back to itself.
 * 2. Follower - Sync request to disk and forward request to
 * SendAckRequestProcessor which send the packets to leader.
 * SendAckRequestProcessor is flushable which allow us to force
 * push packets to leader.
 * 3. Observer - Sync committed request to disk (received as INFORM packet).
 * It never send ack back to the leader, so the nextProcessor will
 * be null. This change the semantic of txnlog on the observer
 * since it only contains committed txns.
 * <p>
 * 1.将事务请求记录到事务日志文件中去
 * 2.触发Zookeeper进行数据快照(进行数据快照时会将原有的事务日志文件输出流置null,这样下次写事务日志时创建新的事务日志文件)
 */
public class SyncRequestProcessor extends ZooKeeperCriticalThread implements
        RequestProcessor {
    private static final Logger LOG = LoggerFactory.getLogger(SyncRequestProcessor.class);
    private final ZooKeeperServer zks;
    private final RequestProcessor nextProcessor;
    /**
     * 请求队列
     */
    private final LinkedBlockingQueue<Request> queuedRequests =
            new LinkedBlockingQueue<>();

    /**
     * 执行快照的线程
     */
    private Thread snapInProcess = null;
    /**
     * 是否在运行
     */
    volatile private boolean running;

    /**
     * Transactions that have been written and are waiting to be flushed to
     * disk. Basically this is the list of SyncItems whose callbacks will be
     * invoked after flush returns successfully.
     * <p>
     * 等待被刷到磁盘的请求队列
     */
    private final LinkedList<Request> toFlush = new LinkedList<>();
    private final Random r = new Random(System.nanoTime());
    /**
     * 两次数据快照之间的事务操作次数
     * The number of log entries to log before starting a snapshot
     */
    private static int snapCount = ZooKeeperServer.getSnapCount();

    /**
     * 结束请求标识
     */
    private final Request requestOfDeath = Request.requestOfDeath;

    public SyncRequestProcessor(ZooKeeperServer zks,
                                RequestProcessor nextProcessor) {
        super("SyncThread:" + zks.getServerId(), zks
                .getZooKeeperServerListener());
        this.zks = zks;
        this.nextProcessor = nextProcessor;
        running = true;
    }

    /**
     * used by tests to check for changing
     * snapcounts
     *
     * @param count
     */
    public static void setSnapCount(int count) {
        snapCount = count;
    }

    /**
     * used by tests to get the snapcount
     *
     * @return the snapcount
     */
    public static int getSnapCount() {
        return snapCount;
    }

    /**
     * 消费请求队列,批处理进行快照以及刷到事务日志
     */
    @Override
    public void run() {
        try {
            //记录上次生成快照文件和事务日志文件之后发生的事务次数
            int logCount = 0;

            // we do this in an attempt to ensure that not all of the servers
            // in the ensemble take a snapshot at the same time
            //防止集群中所有机器在同一时刻进行数据快照,对是否进行数据快照增加随机因素
            int randRoll = r.nextInt(snapCount / 2);
            while (true) {
                Request si;
                if (toFlush.isEmpty()) {
                    //没有要刷到磁盘的请求
                    //消费请求队列(此方法会阻塞)
                    si = queuedRequests.take();
                } else {
                    //有需要刷盘的请求
                    si = queuedRequests.poll();
                    if (si == null) {
                        //如果请求队列的当前请求为空就刷到磁盘
                        // 可以看出,刷新request的优先级不高,只有在queuedRequests为空时才刷新
                        flush(toFlush);
                        continue;
                    }
                }
                //调用shutdown()时,将requestOfDeath放入queuedRequest队列中
                if (si == requestOfDeath) {
                    break;
                }
                if (si != null) {
                    // track the number of records written to the log
                    //请求添加至日志文件，只有事务性请求才会返回true
                    if (zks.getZKDatabase().append(si)) {
                        logCount++;
                        //1.确定是否需要进行数据快照
                        if (logCount > (snapCount / 2 + randRoll)) {
                            randRoll = r.nextInt(snapCount / 2);
                            // roll the log
                            //2.事务日志滚动到另外一个文件(即将当前事务日志关联输出流置null)
                            zks.getZKDatabase().rollLog();
                            // take a snapshot
                            if (snapInProcess != null && snapInProcess.isAlive()) {
                                LOG.warn("Too busy to snap, skipping");
                            } else {
                                //3.创建数据快照异步线程
                                snapInProcess = new ZooKeeperThread("Snapshot Thread") {
                                    @Override
                                    public void run() {
                                        try {
                                            zks.takeSnapshot();
                                        } catch (Exception e) {
                                            LOG.warn("Unexpected exception", e);
                                        }
                                    }
                                };
                                snapInProcess.start();
                            }
                            logCount = 0;
                        }
                    } else if (toFlush.isEmpty()) {
                        // optimization for read heavy workloads
                        // iff this is a read, and there are no pending
                        // flushes (writes), then just pass this to the next
                        // processor
                        if (nextProcessor != null) {
                            nextProcessor.processRequest(si);
                            if (nextProcessor instanceof Flushable) {
                                ((Flushable) nextProcessor).flush();
                            }
                        }
                        continue;
                    }
                    //添加置刷新队列
                    toFlush.add(si);
                    //积攒了过多待刷新请求,直接刷新
                    if (toFlush.size() > 1000) {
                        flush(toFlush);
                    }
                }
            }
        } catch (Throwable t) {
            handleException(this.getName(), t);
        } finally {
            running = false;
        }
        LOG.info("SyncRequestProcessor exited!");
    }

    /**
     * 批处理的思想，把事务日志刷到磁盘，让下一个处理器处理
     *
     * @param toFlush 待刷新的request
     * @throws IOException
     * @throws RequestProcessorException
     */
    private void flush(LinkedList<Request> toFlush)
            throws IOException, RequestProcessorException {
        if (toFlush.isEmpty()) {
            return;
        }
        //先将事务日志刷到磁盘
        zks.getZKDatabase().commit();
        while (!toFlush.isEmpty()) {
            Request i = toFlush.remove();
            if (nextProcessor != null) {
                //交由下一个RequestProcessor处理
                nextProcessor.processRequest(i);
            }
        }
        if (nextProcessor instanceof Flushable) {
            ((Flushable) nextProcessor).flush();
        }
    }

    @Override
    public void shutdown() {
        LOG.info("Shutting down");
        //队列添加requestOfDeath请求
        queuedRequests.add(requestOfDeath);
        try {
            //等待线程结束
            if (running) {
                this.join();
            }
            //调用flush函数
            if (!toFlush.isEmpty()) {
                flush(toFlush);
            }
        } catch (InterruptedException e) {
            LOG.warn("Interrupted while wating for " + this + " to finish");
        } catch (IOException e) {
            LOG.warn("Got IO exception during shutdown");
        } catch (RequestProcessorException e) {
            LOG.warn("Got request processor exception during shutdown");
        }
        //最后关闭nextProcessor
        if (nextProcessor != null) {
            nextProcessor.shutdown();
        }
    }

    /**
     * @param request 将要被处理的request
     */
    @Override
    public void processRequest(Request request) {
        // request.addRQRec(">sync");
        //将request加入请求队列
        queuedRequests.add(request);
    }

}
