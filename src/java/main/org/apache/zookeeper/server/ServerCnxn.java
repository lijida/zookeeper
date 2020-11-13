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

import org.apache.jute.Record;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.data.Id;
import org.apache.zookeeper.proto.ReplyHeader;
import org.apache.zookeeper.proto.RequestHeader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.security.cert.Certificate;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Interface to a Server connection - represents a connection from a client
 * to the server.
 */
public abstract class ServerCnxn implements Stats, Watcher {
    /**
     * This is just an arbitrary object to represent requests issued by (aka owned by) this class
     * 若发生会话转移,则此值不一致
     */
    final public static Object me = new Object();
    private static final Logger LOG = LoggerFactory.getLogger(ServerCnxn.class);

    /**
     * 认证信息
     */
    private Set<Id> authInfo = Collections.newSetFromMap(new ConcurrentHashMap<>());

    /**
     * If the client is of old version, we don't send r-o mode info to it.
     * The reason is that if we would, old C client doesn't read it, which
     * results in TCP RST packet, i.e. "connection reset by peer".
     * <p>
     * 是否为旧的客户端
     */
    boolean isOldClient = true;

    /**
     * @return 会话超时时间
     */
    abstract int getSessionTimeout();

    /**
     * 关闭连接
     */
    abstract void close();

    /**
     * 发送响应
     *
     * @param h   响应头
     * @param r   响应体
     * @param tag 标记
     * @throws IOException
     */
    public abstract void sendResponse(ReplyHeader h, Record r, String tag)
            throws IOException;

    /**
     * notify the client the session is closing and close/cleanup socket
     * 关闭会话
     */
    abstract void sendCloseSession();

    /**
     * {@link Watcher}接口中的方法
     *
     * @param event 事件类型
     */
    @Override
    public abstract void process(WatchedEvent event);

    /**
     * @return 会话id
     */
    public abstract long getSessionId();

    /**
     * 设置会话id
     *
     * @param sessionId 会话id
     */
    abstract void setSessionId(long sessionId);

    /**
     * auth info for the cnxn, returns an unmodifyable list
     * 获取认证信息，返回不可变的列表
     */
    public List<Id> getAuthInfo() {
        return Collections.unmodifiableList(new ArrayList<>(authInfo));
    }

    /**
     * 添加认证信息
     *
     * @param id 认证信息
     */
    public void addAuthInfo(Id id) {
        authInfo.add(id);
    }

    /**
     * 移除认证信息
     *
     * @param id 认证信息
     * @return 是否成功移除
     */
    public boolean removeAuthInfo(Id id) {
        return authInfo.remove(id);
    }

    /**
     * 发送数据,此方法是请求处理链将数据发送给网络IO的接口
     *
     * @param closeConn 待发送的数据
     */
    abstract void sendBuffer(ByteBuffer closeConn);

    /**
     * 允许接收新数据
     */
    abstract void enableRecv();

    /**
     * 不允许接收客户端的数据
     */
    abstract void disableRecv();

    /**
     * 设置会话超时时间
     *
     * @param sessionTimeout 会话超时时间
     */
    abstract void setSessionTimeout(int sessionTimeout);

    /**
     * ZooKeeper的Sasl服务器
     */
    protected ZooKeeperSaslServer zooKeeperSaslServer = null;

    /**
     * 请求关闭异常类
     */
    protected static class CloseRequestException extends IOException {
        private static final long serialVersionUID = -7854505709816442681L;

        public CloseRequestException(String msg) {
            super(msg);
        }
    }

    /**
     * 流结束异常类
     */
    protected static class EndOfStreamException extends IOException {
        private static final long serialVersionUID = -8255690282104294178L;

        public EndOfStreamException(String msg) {
            super(msg);
        }

        @Override
        public String toString() {
            return "EndOfStreamException: " + getMessage();
        }
    }

    /**
     * 修改统计值,当接收一个packet时此方法被调用
     */
    protected void packetReceived() {
        incrPacketsReceived();
        ServerStats serverStats = serverStats();
        if (serverStats != null) {
            serverStats().incrementPacketsReceived();
        }
    }

    /**
     * 修改统计值,当发送一个packet时此方法被调用
     */
    protected void packetSent() {
        incrPacketsSent();
        ServerStats serverStats = serverStats();
        if (serverStats != null) {
            serverStats().incrementPacketsSent();
        }
    }

    /**
     * @return 服务器统计信息
     */
    protected abstract ServerStats serverStats();

    /**
     * 连接创建时间
     */
    protected final Date established = new Date();

    /**
     * 统计值,已接收的packet数
     */
    protected final AtomicLong packetsReceived = new AtomicLong();
    /**
     * 统计值,已发送的packet数
     */
    protected final AtomicLong packetsSent = new AtomicLong();

    /**
     * 最小延迟
     */
    protected long minLatency;
    /**
     * 最大延迟
     */
    protected long maxLatency;
    /**
     * 最后操作类型
     */
    protected String lastOp;
    /**
     * 最后的cxid
     */
    protected long lastCxid;
    /**
     * 最后的zxid
     */
    protected long lastZxid;
    /**
     * 最后的响应时间
     */
    protected long lastResponseTime;
    /**
     * 最后的延迟
     */
    protected long lastLatency;

    /**
     * 总请求数量
     */
    protected long count;
    /**
     * 总延迟
     */
    protected long totalLatency;

    /**
     * 重置统计数据
     */
    @Override
    public synchronized void resetStats() {
        packetsReceived.set(0);
        packetsSent.set(0);
        minLatency = Long.MAX_VALUE;
        maxLatency = 0;
        lastOp = "NA";
        lastCxid = -1;
        lastZxid = -1;
        lastResponseTime = 0;
        lastLatency = 0;

        count = 0;
        totalLatency = 0;
    }

    /**
     * 增加接收的packet数量
     */
    protected long incrPacketsReceived() {
        return packetsReceived.incrementAndGet();
    }

    /**
     * 增加outstandingRequest数量
     *
     * @param h
     */
    protected void incrOutstandingRequests(RequestHeader h) {
    }

    /**
     * 增加发送的packet数量
     *
     * @return
     */
    protected long incrPacketsSent() {
        return packetsSent.incrementAndGet();
    }

    /**
     * 更新响应的统计数据
     *
     * @param cxid
     * @param zxid
     * @param op
     * @param start
     * @param end
     */
    protected synchronized void updateStatsForResponse(long cxid, long zxid,
                                                       String op, long start, long end) {
        // don't overwrite with "special" xids - we're interested
        // in the clients last real operation
        if (cxid >= 0) {
            lastCxid = cxid;
        }
        lastZxid = zxid;
        lastOp = op;
        lastResponseTime = end;
        long elapsed = end - start;
        lastLatency = elapsed;
        if (elapsed < minLatency) {
            minLatency = elapsed;
        }
        if (elapsed > maxLatency) {
            maxLatency = elapsed;
        }
        count++;
        totalLatency += elapsed;
    }

    @Override
    public Date getEstablished() {
        return (Date) established.clone();
    }

    @Override
    public abstract long getOutstandingRequests();

    @Override
    public long getPacketsReceived() {
        return packetsReceived.longValue();
    }

    @Override
    public long getPacketsSent() {
        return packetsSent.longValue();
    }

    @Override
    public synchronized long getMinLatency() {
        return minLatency == Long.MAX_VALUE ? 0 : minLatency;
    }

    @Override
    public synchronized long getAvgLatency() {
        return count == 0 ? 0 : totalLatency / count;
    }

    @Override
    public synchronized long getMaxLatency() {
        return maxLatency;
    }

    @Override
    public synchronized String getLastOperation() {
        return lastOp;
    }

    @Override
    public synchronized long getLastCxid() {
        return lastCxid;
    }

    @Override
    public synchronized long getLastZxid() {
        return lastZxid;
    }

    @Override
    public synchronized long getLastResponseTime() {
        return lastResponseTime;
    }

    @Override
    public synchronized long getLastLatency() {
        return lastLatency;
    }

    /**
     * Prints detailed stats information for the connection.
     *
     * @see dumpConnectionInfo(PrintWriter, boolean) for brief stats
     */
    @Override
    public String toString() {
        StringWriter sw = new StringWriter();
        PrintWriter pwriter = new PrintWriter(sw);
        dumpConnectionInfo(pwriter, false);
        pwriter.flush();
        pwriter.close();
        return sw.toString();
    }

    public abstract InetSocketAddress getRemoteSocketAddress();

    public abstract int getInterestOps();

    public abstract boolean isSecure();

    public abstract Certificate[] getClientCertificateChain();

    public abstract void setClientCertificateChain(Certificate[] chain);

    /**
     * Print information about the connection.
     *
     * @param brief iff true prints brief details, otw full detail
     * @return information about this connection
     */
    public synchronized void
    dumpConnectionInfo(PrintWriter pwriter, boolean brief) {
        pwriter.print(" ");
        pwriter.print(getRemoteSocketAddress());
        pwriter.print("[");
        int interestOps = getInterestOps();
        pwriter.print(interestOps == 0 ? "0" : Integer.toHexString(interestOps));
        pwriter.print("](queued=");
        pwriter.print(getOutstandingRequests());
        pwriter.print(",recved=");
        pwriter.print(getPacketsReceived());
        pwriter.print(",sent=");
        pwriter.print(getPacketsSent());

        if (!brief) {
            long sessionId = getSessionId();
            if (sessionId != 0) {
                pwriter.print(",sid=0x");
                pwriter.print(Long.toHexString(sessionId));
                pwriter.print(",lop=");
                pwriter.print(getLastOperation());
                pwriter.print(",est=");
                pwriter.print(getEstablished().getTime());
                pwriter.print(",to=");
                pwriter.print(getSessionTimeout());
                long lastCxid = getLastCxid();
                if (lastCxid >= 0) {
                    pwriter.print(",lcxid=0x");
                    pwriter.print(Long.toHexString(lastCxid));
                }
                pwriter.print(",lzxid=0x");
                pwriter.print(Long.toHexString(getLastZxid()));
                pwriter.print(",lresp=");
                pwriter.print(getLastResponseTime());
                pwriter.print(",llat=");
                pwriter.print(getLastLatency());
                pwriter.print(",minlat=");
                pwriter.print(getMinLatency());
                pwriter.print(",avglat=");
                pwriter.print(getAvgLatency());
                pwriter.print(",maxlat=");
                pwriter.print(getMaxLatency());
            }
        }
        pwriter.print(")");
    }

    /**
     * @param brief 是否只返回简要信息
     * @return 本连接的相关信息
     */
    public synchronized Map<String, Object> getConnectionInfo(boolean brief) {
        Map<String, Object> info = new LinkedHashMap<>();
        info.put("remote_socket_address", getRemoteSocketAddress());
        info.put("interest_ops", getInterestOps());
        info.put("outstanding_requests", getOutstandingRequests());
        info.put("packets_received", getPacketsReceived());
        info.put("packets_sent", getPacketsSent());
        if (!brief) {
            info.put("session_id", getSessionId());
            info.put("last_operation", getLastOperation());
            info.put("established", getEstablished());
            info.put("session_timeout", getSessionTimeout());
            info.put("last_cxid", getLastCxid());
            info.put("last_zxid", getLastZxid());
            info.put("last_response_time", getLastResponseTime());
            info.put("last_latency", getLastLatency());
            info.put("min_latency", getMinLatency());
            info.put("avg_latency", getAvgLatency());
            info.put("max_latency", getMaxLatency());
        }
        return info;
    }

    /**
     * clean up the socket related to a command and also make sure we flush the
     * data before we do that
     *
     * @param pwriter the pwriter for a command socket
     */
    public void cleanupWriterSocket(PrintWriter pwriter) {
        try {
            if (pwriter != null) {
                pwriter.flush();
                pwriter.close();
            }
        } catch (Exception e) {
            LOG.info("Error closing PrintWriter ", e);
        } finally {
            try {
                close();
            } catch (Exception e) {
                LOG.error("Error closing a command socket ", e);
            }
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ServerCnxn that = (ServerCnxn) o;
        return isOldClient == that.isOldClient &&
                minLatency == that.minLatency &&
                maxLatency == that.maxLatency &&
                lastCxid == that.lastCxid &&
                lastZxid == that.lastZxid &&
                lastResponseTime == that.lastResponseTime &&
                lastLatency == that.lastLatency &&
                count == that.count &&
                totalLatency == that.totalLatency &&
                Objects.equals(authInfo, that.authInfo) &&
                Objects.equals(zooKeeperSaslServer, that.zooKeeperSaslServer) &&
                Objects.equals(established, that.established) &&
                Objects.equals(packetsReceived, that.packetsReceived) &&
                Objects.equals(packetsSent, that.packetsSent) &&
                Objects.equals(lastOp, that.lastOp);
    }

    @Override
    public int hashCode() {

        return Objects.hash(authInfo, isOldClient, zooKeeperSaslServer, established, packetsReceived, packetsSent, minLatency, maxLatency, lastOp, lastCxid, lastZxid, lastResponseTime, lastLatency, count, totalLatency);
    }
}
