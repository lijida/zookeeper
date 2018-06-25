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

import org.apache.jute.BinaryInputArchive;
import org.apache.jute.BinaryOutputArchive;
import org.apache.jute.Record;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.data.Id;
import org.apache.zookeeper.proto.ReplyHeader;
import org.apache.zookeeper.proto.RequestHeader;
import org.apache.zookeeper.proto.WatcherEvent;
import org.apache.zookeeper.server.NIOServerCnxnFactory.SelectorThread;
import org.apache.zookeeper.server.command.CommandExecutor;
import org.apache.zookeeper.server.command.FourLetterCommands;
import org.apache.zookeeper.server.command.NopCommand;
import org.apache.zookeeper.server.command.SetTraceMaskCommand;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.CancelledKeyException;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.security.cert.Certificate;
import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * This class handles communication with clients using NIO. There is one per
 * client, but only one thread doing the communication.
 * <p>
 * 封装客户端的连接
 */
public class NIOServerCnxn extends ServerCnxn {
    private static final Logger LOG = LoggerFactory.getLogger(NIOServerCnxn.class);

    /**
     * 关联的NIOServerCnxnFactory
     */
    private final NIOServerCnxnFactory factory;

    /**
     * 封装的客户端连接
     */
    private final SocketChannel sock;

    private final SelectorThread selectorThread;

    /**
     * {@link #sock}注册到selector后关联的SelectionKey
     */
    private final SelectionKey sk;

    /**
     * 标识NIOServerCnxn是否初始化,server处理了客户端的"会话创建"请求后,才算初始化完成
     */
    private boolean initialized;
    /**
     * 分配四个字节缓冲区
     */
    private final ByteBuffer lenBuffer = ByteBuffer.allocate(4);

    private ByteBuffer incomingBuffer = lenBuffer;

    /**
     * 待发送的数据队列
     */
    private final Queue<ByteBuffer> outgoingBuffers =
            new LinkedBlockingQueue<>();

    /**
     * 会话超时时间
     */
    private int sessionTimeout;

    /**
     * 关联的ZooKeeperServer
     */
    private final ZooKeeperServer zkServer;

    /**
     * The number of requests that have been submitted but not yet responded to.
     * <p>
     * 该连接上已经被提交但还未响应的请求数量
     */
    private final AtomicInteger outstandingRequests = new AtomicInteger(0);

    /**
     * This is the id that uniquely identifies the session of a client. Once
     * this session is no longer active, the ephemeral nodes will go away.
     * <p>
     * 会话id
     */
    private long sessionId;

    private final int outstandingLimit;

    public NIOServerCnxn(ZooKeeperServer zk, SocketChannel sock,
                         SelectionKey sk, NIOServerCnxnFactory factory,
                         SelectorThread selectorThread) throws IOException {
        this.zkServer = zk;
        this.sock = sock;
        this.sk = sk;
        this.factory = factory;
        this.selectorThread = selectorThread;
        if (this.factory.login != null) {
            this.zooKeeperSaslServer = new ZooKeeperSaslServer(factory.login);
        }
        if (zk != null) {
            outstandingLimit = zk.getGlobalOutstandingLimit();
        } else {
            outstandingLimit = 1;
        }
        //TCP_NODEALY的默认值为false,表示采用Negale算法.
        // 如果调用setTcpNoDelay(true)方法,就会关闭Socket的缓冲,确保数据及时发送
        sock.socket().setTcpNoDelay(true);
        /* set socket linger to false, so that socket close does not block */
        /*
        当我们调用Socket的close方法时,默认的行为是当底层网卡所有数据都发送完毕后,关闭连接.
        通过setSoLinger方法，我们可以修改close方法的行为.
        1，setSoLinger(true, 0)
        当网卡收到关闭连接请求后，无论数据是否发送完毕，立即发送RST包关闭连接
        2，setSoLinger(true, delay_time)
        当网卡收到关闭连接请求后，等待delay_time，
        如果在delay_time过程中数据发送完毕，正常四次挥手关闭连接；
        如果在delay_time过程中数据没有发送完毕，发送RST包关闭连接
         */
        sock.socket().setSoLinger(false, -1);
        // 获取IP地址
        InetAddress addr = ((InetSocketAddress) sock.socket()
                .getRemoteSocketAddress()).getAddress();
        //认证信息中添加IP地址
        addAuthInfo(new Id("ip", addr.getHostAddress()));
        this.sessionTimeout = factory.sessionlessCnxnTimeout;
    }

    /**
     * Send close connection packet to the client, doIO will eventually
     * close the underlying machinery (like socket, selectorkey, etc...)
     */
    @Override
    public void sendCloseSession() {
        sendBuffer(ServerCnxnFactory.closeConn);
    }

    /**
     * send buffer without using the asynchronous
     * calls to selector and then close the socket
     *
     * @param bb
     */
    void sendBufferSync(ByteBuffer bb) {
        try {
            /* configure socket to be blocking so that we dont have to do write in
             * a tight while loop
             */
            if (bb != ServerCnxnFactory.closeConn) {
                if (sock.isOpen()) {
                    sock.configureBlocking(true);
                    sock.write(bb);
                }
                packetSent();
            }
        } catch (IOException ie) {
            LOG.error("Error sending data synchronously ", ie);
        }
    }

    /**
     * sendBuffer pushes a byte buffer onto the outgoing buffer queue for
     * asynchronous writes.
     */
    @Override
    public void sendBuffer(ByteBuffer bb) {
        if (LOG.isTraceEnabled()) {
            LOG.trace("Add a buffer to outgoingBuffers, sk " + sk
                    + " is valid: " + sk.isValid());
        }
        outgoingBuffers.add(bb);
        requestInterestOpsUpdate();
    }

    /**
     * 有两种情况会调用此方法:
     * 1.根据lengthBuffer的值为incomingBuffer分配空间后,此时尚未将数据从socketChannel读取至incomingBuffer中
     * 2.已经将数据从socketChannel中读取至incomingBuffer,且读取完毕
     * <p>
     * Read the request payload (everything following the length prefix)
     */
    private void readPayload() throws IOException, InterruptedException {
        // have we read length bytes?
        if (incomingBuffer.remaining() != 0) {
            // sock is non-blocking, so ok
            //对应情况1,此时刚为incomingBuffer分配空间,incomingBuffer为空,进行一次数据读取
            //(1)若将incomingBuffer读满,则直接进行处理;
            //(2)若未将incomingBuffer读满,则说明此次发送的数据不能构成一个完整的请求,则等待下一次数据到达后调用doIo()时再次将数据
            //从socketChannel读取至incomingBuffer
            int rc = sock.read(incomingBuffer);
            if (rc < 0) {
                throw new EndOfStreamException(
                        "Unable to read additional data from client sessionid 0x"
                                + Long.toHexString(sessionId)
                                + ", likely client has closed socket");
            }
        }
        // have we read length bytes?
        if (incomingBuffer.remaining() == 0) {
            //不管是情况1还是情况2,此时incomingBuffer已读满,其中内容必是一个request,处理该request
            //更新统计值
            packetReceived();
            incomingBuffer.flip();
            if (!initialized) {
                //处理连接请求
                readConnectRequest();
            } else {
                //处理普通请求
                readRequest();
            }
            //请求处理结束,重置lenBuffer和incomingBuffer
            lenBuffer.clear();
            incomingBuffer = lenBuffer;
        }
    }

    /**
     * 由于selector.select()是水平触发,只要socketChannel中有数据需要读,就会一直被select.select()到,
     * 但若此时正在读取数据的过程中,数据尚未被读取完毕,此时会被select.select()到,为了保证效率,在处理数据的过程中,
     * 即使被selector.select()到,也不用处理此次事件,因此该标志在处理数据时设置为false,数据读取完成后,设置为true
     * <p>
     * This boolean tracks whether the connection is ready for selection or
     * not. A connection is marked as not ready for selection while it is
     * processing an IO request. The flag is used to gate keep pushing interest
     * op updates onto the selector.
     */
    private final AtomicBoolean selectable = new AtomicBoolean(true);

    public boolean isSelectable() {
        return sk.isValid() && selectable.get();
    }

    public void disableSelectable() {
        selectable.set(false);
    }

    public void enableSelectable() {
        selectable.set(true);
    }

    private void requestInterestOpsUpdate() {
        if (isSelectable()) {
            selectorThread.addInterestOpsUpdateRequest(sk);
        }
    }

    /**
     * 当{@link #sock}可写时调用该方法
     *
     * @param k {@link #sock}关联的SelectionKey
     * @throws IOException
     * @throws CloseRequestException
     */
    void handleWrite(SelectionKey k) throws IOException, CloseRequestException {
        if (outgoingBuffers.isEmpty()) {
            return;
        }

        /*
         * 设置position=0,limit=capacity,这样就可以将需要发送的数据从非直接内存填充至直接内存.
         *
         * This is going to reset the buffer position to 0 and the
         * limit to the size of the buffer, so that we can fill it
         * with data from the non-direct buffers that we need to
         * send.
         */
        ByteBuffer directBuffer = NIOServerCnxnFactory.getDirectBuffer();
        if (directBuffer == null) {
            //不使用直接内存
            ByteBuffer[] bufferList = new ByteBuffer[outgoingBuffers.size()];
            // Use gathered write call. This updates the positions of the
            // byte buffers to reflect the bytes that were written out.
            sock.write(outgoingBuffers.toArray(bufferList));

            // Remove the buffers that we have sent
            ByteBuffer bb;
            while ((bb = outgoingBuffers.peek()) != null) {
                if (bb == ServerCnxnFactory.closeConn) {
                    throw new CloseRequestException("close requested");
                }
                if (bb.remaining() > 0) {
                    break;
                }
                packetSent();
                outgoingBuffers.remove();
            }
        } else {
            //使用直接内存
            directBuffer.clear();

            for (ByteBuffer b : outgoingBuffers) {
                if (directBuffer.remaining() < b.remaining()) {
                    /*
                     * 若directBuffer的剩余可写空间不足以容纳b的所有数据,则修改b的limit为directBuffer的剩余可写空间.
                     * 这样下面的复制代码刚好将directBuffer的可写空间写满
                     *
                     * When we call put later, if the directBuffer is to
                     * small to hold everything, nothing will be copied,
                     * so we've got to slice the buffer if it's too big.
                     */
                    b = (ByteBuffer) b.slice().limit(directBuffer.remaining());
                }
                /*
                 * put()会修改b和directBuffer的position值,但是我们不能修改b的position值,
                 * 因为下文需要position的值将已发送的数据移出outgoingBuffers,因此在复制结束后重置position值.
                 *
                 * put() is going to modify the positions of both
                 * buffers, but we don't want to change the position of
                 * the source buffers (we'll do that after the send, if
                 * needed), so we save and reset the position after the
                 * copy
                 */
                int p = b.position();
                //将b中的数据复制到directBuffer中
                directBuffer.put(b);
                b.position(p);
                if (directBuffer.remaining() == 0) {
                    break;
                }
            }
            /*
             * Do the flip: limit becomes position, position gets set to
             * 0. This sets us up for the write.
             */
            directBuffer.flip();

            //返回发送的字节数,下文据此移除已发送的数据
            int sent = sock.write(directBuffer);

            ByteBuffer bb;

            // Remove the buffers that we have sent
            // 将已发送的buffers从outgoingBuffers中移除
            while ((bb = outgoingBuffers.peek()) != null) {
                if (bb == ServerCnxnFactory.closeConn) {
                    throw new CloseRequestException("close requested");
                }
                if (sent < bb.remaining()) {
                    /*
                     * 只发送了此Buffer的部分数据,因此修改position的值并退出循环
                     *
                     * We only partially sent this buffer, so we update
                     * the position and exit the loop.
                     */
                    bb.position(bb.position() + sent);
                    break;
                }
                packetSent();
                /* We've sent the whole buffer, so drop the buffer */
                //该buffer的数据已经全部发送,将buffer从outgoingBuffers中移除
                sent -= bb.remaining();
                outgoingBuffers.remove();
            }
        }
    }

    /**
     * Only used in order to allow testing
     */
    protected boolean isSocketOpen() {
        return sock.isOpen();
    }

    /**
     * Handles read/write IO on connection.
     */
    void doIO(SelectionKey k) throws InterruptedException {
        try {
            if (!isSocketOpen()) {
                LOG.warn("trying to do i/o on a null socket for session:0x"
                        + Long.toHexString(sessionId));

                return;
            }
           /*
            处理读操作的流程
            1.最开始incomingBuffer就是lenBuffer,容量为4.第一次读取4个字节,即此次请求报文的长度
            2.根据请求报文的长度分配incomingBuffer的大小
            3.将读到的字节存放在incomingBuffer中,直至读满
             (由于第2步中为incomingBuffer分配的长度刚好是报文的长度,此时incomingBuffer中刚好时一个报文)
            4.处理报文
            */
            if (k.isReadable()) {
                //若是客户端请求,此时触发读事件
                //初始化时incomingBuffer即时lengthBuffer,只分配了4个字节,供用户读取一个int(此int值就是此次请求报文的总长度)
                int rc = sock.read(incomingBuffer);
                if (rc < 0) {
                    throw new EndOfStreamException(
                            "Unable to read additional data from client sessionid 0x"
                                    + Long.toHexString(sessionId)
                                    + ", likely client has closed socket");
                }
                /*
                只有incomingBuffer.remaining() == 0,才会进行下一步的处理,否则一直读取数据直到incomingBuffer读满,此时有两种可能:
                1.incomingBuffer就是lenBuffer,此时incomingBuffer的内容是此次请求报文的长度.
                根据lenBuffer为incomingBuffer分配空间后调用readPayload().
                在readPayload()中会立马进行一次数据读取,(1)若可以将incomingBuffer读满,则incomingBuffer中就是一个完整的请求,处理该请求;
                (2)若不能将incomingBuffer读满,说明出现了拆包问题,此时不能构造一个完整的请求,只能等待客户端继续发送数据,等到下次socketChannel可读时,继续将数据读取到incomingBuffer中
                2.incomingBuffer不是lenBuffer,说明上次读取时出现了拆包问题,incomingBuffer中只有一个请求的部分数据.
                而这次读取的数据加上上次读取的数据凑成了一个完整的请求,调用readPayload()
                 */

                if (incomingBuffer.remaining() == 0) {
                    boolean isPayload;
                    if (incomingBuffer == lenBuffer) {
                        // start of next request
                        //解析上文中读取的报文总长度,同时为"incomingBuffer"分配len的空间供读取全部报文
                        incomingBuffer.flip();
                        //为incomeingBuffer分配空间时还包括了判断是否是"4字命令"的逻辑
                        isPayload = readLength(k);
                        incomingBuffer.clear();
                    } else {
                        //2.incomingBuffer不是lenBuffer,此时incomingBuffer的内容是payload
                        // continuation
                        isPayload = true;
                    }
                    if (isPayload) {
                        // not the case for 4letterword
                        //处理报文
                        readPayload();
                    } else {
                        // four letter words take care
                        // need not do anything else
                        return;
                    }
                }
            }
            if (k.isWritable()) {
                //处理写操作
                handleWrite(k);

                if (!initialized && !getReadInterest() && !getWriteInterest()) {
                    throw new CloseRequestException("responded to info probe");
                }
            }
        } catch (CancelledKeyException e) {
            LOG.warn("CancelledKeyException causing close of session 0x"
                    + Long.toHexString(sessionId));
            if (LOG.isDebugEnabled()) {
                LOG.debug("CancelledKeyException stack trace", e);
            }
            close();
        } catch (CloseRequestException e) {
            // expecting close to log session closure
            close();
        } catch (EndOfStreamException e) {
            LOG.warn(e.getMessage());
            // expecting close to log session closure
            close();
        } catch (IOException e) {
            LOG.warn("Exception causing close of session 0x"
                    + Long.toHexString(sessionId) + ": " + e.getMessage());
            if (LOG.isDebugEnabled()) {
                LOG.debug("IOException stack trace", e);
            }
            close();
        }
    }

    /**
     * 处理普通请求
     *
     * @throws IOException
     */
    private void readRequest() throws IOException {
        zkServer.processPacket(this, incomingBuffer);
    }


    /**
     * Only called as callback from {@link ZooKeeperServer#processPacket(ServerCnxn, ByteBuffer)}
     *
     * @param h
     */
    @Override
    protected void incrOutstandingRequests(RequestHeader h) {
        if (h.getXid() >= 0) {
            outstandingRequests.incrementAndGet();
            // check throttling
            int inProcess = zkServer.getInProcess();
            if (inProcess > outstandingLimit) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Throttling recv " + inProcess);
                }
                disableRecv();
            }
        }
    }


    /**
     * 返回是否监听OP_WRITE事件,若有待发送的数据,则监听OP_WRITE事件
     *
     * @return whether we are interested in writing, which is determined
     * by whether we have any pending buffers on the output queue or not
     */
    private boolean getWriteInterest() {
        return !outgoingBuffers.isEmpty();
    }


    /**
     * 返回是否监听OP_READ事件,若不节流,则监听OP_READ事件
     *
     * @return whether we are interested in taking new requests, which is
     * determined by whether we are currently throttled or not
     */
    private boolean getReadInterest() {
        return !throttled.get();
    }

    private final AtomicBoolean throttled = new AtomicBoolean(false);


    /**
     * 执行节流策略,不再接受新request,如果需要进行状态更改,则向选择器注册一个interest op更新请求
     * <p>
     * Throttle acceptance of new requests. If this entailed a state change,
     * register an interest op update request with the selector.
     */
    @Override
    public void disableRecv() {
        if (throttled.compareAndSet(false, true)) {
            requestInterestOpsUpdate();
        }
    }


    /**
     * 关闭节流策略,重新接受新请求.如果需要进行状态更改,则向选择器注册一个interest op更新请求。
     * <p>
     * Disable throttling(节流) and resume acceptance of new requests. If this
     * entailed a state change, register an interest op update request with
     * the selector.
     */
    @Override
    public void enableRecv() {
        if (throttled.compareAndSet(true, false)) {
            requestInterestOpsUpdate();
        }
    }

    private void readConnectRequest() throws IOException, InterruptedException {
        if (!isZKServerRunning()) {
            throw new IOException("ZooKeeperServer not running");
        }
        zkServer.processConnectRequest(this, incomingBuffer);
        initialized = true;
    }

    /**
     * 这个类封装了NIOServerCnxn的sendBuffer方法。它负责对客户的响应进行分块处理。
     * 这个类并没有在内存中完整地构造响应(对于某些命令来说可能比较大)，而是将结果块化。
     * <p>
     * This class wraps the sendBuffer method of NIOServerCnxn. It is
     * responsible for chunking up the response to a client. Rather
     * than cons'ing up a response fully in memory, which may be large
     * for some commands, this class chunks up the result.
     */
    private class SendBufferWriter extends Writer {
        private StringBuffer sb = new StringBuffer();

        /**
         * Check if we are ready to send another chunk.
         * 检查是否准备好发送另一块
         *
         * @param force force sending, even if not a full chunk
         */
        private void checkFlush(boolean force) {
            //满足以下两个条件之一,就发送数据:
            //1.需要强制发送时，sb缓冲中只要有内容就会同步发送
            //2.当sb的大小超过2048（块）时就需要发送
            if ((force && sb.length() > 0) || sb.length() > 2048) {
                sendBufferSync(ByteBuffer.wrap(sb.toString().getBytes()));
                // clear our internal buffer
                sb.setLength(0);
            }
        }

        @Override
        public void close() throws IOException {
            if (sb == null) {
                return;
            }
            // 关闭之前需要强制性发送缓存
            checkFlush(true);
            // clear out the ref to ensure no reuse
            sb = null;
        }

        @Override
        public void flush() throws IOException {
            checkFlush(true);
        }

        @Override
        public void write(char[] cbuf, int off, int len) throws IOException {
            sb.append(cbuf, off, len);
            checkFlush(false);
        }
    }

    /**
     * Return if four letter word found and responded to, otw false
     **/
    private boolean checkFourLetterWord(final SelectionKey k, final int len)
            throws IOException {
        // We take advantage of the limited size of the length to look
        // for cmds. They are all 4-bytes which fits inside of an int
        if (!FourLetterCommands.isKnown(len)) {
            return false;
        }

        String cmd = FourLetterCommands.getCommandString(len);
        packetReceived();

        /** cancel the selection key to remove the socket handling
         * from selector. This is to prevent netcat problem wherein
         * netcat immediately closes the sending side after sending the
         * commands and still keeps the receiving channel open.
         * The idea is to remove the selectionkey from the selector
         * so that the selector does not notice the closed read on the
         * socket channel and keep the socket alive to write the data to
         * and makes sure to close the socket after its done writing the data
         */
        if (k != null) {
            try {
                k.cancel();
            } catch (Exception e) {
                LOG.error("Error cancelling command selection key ", e);
            }
        }

        final PrintWriter pwriter = new PrintWriter(
                new BufferedWriter(new SendBufferWriter()));

        // ZOOKEEPER-2693: don't execute 4lw if it's not enabled.
        if (!FourLetterCommands.isEnabled(cmd)) {
            LOG.debug("Command {} is not executed because it is not in the whitelist.", cmd);
            NopCommand nopCmd = new NopCommand(pwriter, this, cmd +
                    " is not executed because it is not in the whitelist.");
            nopCmd.start();
            return true;
        }

        LOG.info("Processing " + cmd + " command from "
                + sock.socket().getRemoteSocketAddress());

        if (len == FourLetterCommands.setTraceMaskCmd) {
            incomingBuffer = ByteBuffer.allocate(8);
            int rc = sock.read(incomingBuffer);
            if (rc < 0) {
                throw new IOException("Read error");
            }
            incomingBuffer.flip();
            long traceMask = incomingBuffer.getLong();
            ZooTrace.setTextTraceLevel(traceMask);
            SetTraceMaskCommand setMask = new SetTraceMaskCommand(pwriter, this, traceMask);
            setMask.start();
            return true;
        } else {
            CommandExecutor commandExecutor = new CommandExecutor();
            return commandExecutor.execute(this, pwriter, len, zkServer, factory);
        }
    }

    /**
     * @return true if the server is running, false otherwise.
     */
    boolean isZKServerRunning() {
        return zkServer != null && zkServer.isRunning();
    }

    /**
     * Reads the first 4 bytes of lenBuffer, which could be true length or
     * four letter word.
     *
     * @param k selection key
     * @return true if length read, otw false (wasn't really the length)
     * @throws IOException if buffer size exceeds maxBuffer size
     */
    private boolean readLength(SelectionKey k) throws IOException {
        // Read the length, now get the buffer
        int len = lenBuffer.getInt();
        if (!initialized && checkFourLetterWord(sk, len)) {
            return false;
        }
        if (len < 0 || len > BinaryInputArchive.maxBuffer) {
            throw new IOException("Len error " + len);
        }
        if (!isZKServerRunning()) {
            throw new IOException("ZooKeeperServer not running");
        }
        incomingBuffer = ByteBuffer.allocate(len);
        return true;
    }

    @Override
    public long getOutstandingRequests() {
        return outstandingRequests.get();
    }

    /*
     * (non-Javadoc)
     *
     * @see org.apache.zookeeper.server.ServerCnxnIface#getSessionTimeout()
     */
    @Override
    public int getSessionTimeout() {
        return sessionTimeout;
    }

    /**
     * Used by "dump" 4-letter command to list all connection in
     * cnxnExpiryMap
     */
    @Override
    public String toString() {
        return "ip: " + sock.socket().getRemoteSocketAddress() +
                " sessionId: 0x" + Long.toHexString(sessionId);
    }

    /**
     * Close the cnxn and remove it from the factory cnxns list.
     */
    @Override
    public void close() {
        if (!factory.removeCnxn(this)) {
            return;
        }

        if (zkServer != null) {
            zkServer.removeCnxn(this);
        }

        if (sk != null) {
            try {
                // need to cancel this selection key from the selector
                sk.cancel();
            } catch (Exception e) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("ignoring exception during selectionkey cancel", e);
                }
            }
        }

        closeSock();
    }

    /**
     * Close resources associated with the sock of this cnxn.
     */
    private void closeSock() {
        if (!sock.isOpen()) {
            return;
        }

        LOG.info("Closed socket connection for client "
                + sock.socket().getRemoteSocketAddress()
                + (sessionId != 0 ?
                " which had sessionid 0x" + Long.toHexString(sessionId) :
                " (no session established for client)"));
        closeSock(sock);
    }

    /**
     * Close resources associated with a sock.
     */
    public static void closeSock(SocketChannel sock) {
        if (!sock.isOpen()) {
            return;
        }

        try {
            /*
             * The following sequence of code is stupid! You would think that
             * only sock.close() is needed, but alas, it doesn't work that way.
             * If you just do sock.close() there are cases where the socket
             * doesn't actually close...
             */
            sock.socket().shutdownOutput();
        } catch (IOException e) {
            // This is a relatively common exception that we can't avoid
            if (LOG.isDebugEnabled()) {
                LOG.debug("ignoring exception during output shutdown", e);
            }
        }
        try {
            sock.socket().shutdownInput();
        } catch (IOException e) {
            // This is a relatively common exception that we can't avoid
            if (LOG.isDebugEnabled()) {
                LOG.debug("ignoring exception during input shutdown", e);
            }
        }
        try {
            sock.socket().close();
        } catch (IOException e) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("ignoring exception during socket close", e);
            }
        }
        try {
            sock.close();
        } catch (IOException e) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("ignoring exception during socketchannel close", e);
            }
        }
    }

    private final static byte fourBytes[] = new byte[4];

    /**
     * (non-Javadoc)
     * 异步发送response,将response序列化为ByteBuffer后添加至{@link #outgoingBuffers}
     *
     * @param h   响应头
     * @param r   响应体
     * @param tag 序列化表情
     * @see org.apache.zookeeper.server.ServerCnxn#sendResponse(org.apache.zookeeper.proto.ReplyHeader,
     * org.apache.jute.Record, java.lang.String)
     */
    @Override
    public void sendResponse(ReplyHeader h, Record r, String tag) {
        try {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            // Make space for length
            BinaryOutputArchive bos = BinaryOutputArchive.getArchive(baos);
            try {
                baos.write(fourBytes);
                bos.writeRecord(h, "header");
                if (r != null) {
                    bos.writeRecord(r, tag);
                }
                baos.close();
            } catch (IOException e) {
                LOG.error("Error serializing response");
            }
            byte b[] = baos.toByteArray();
            ByteBuffer bb = ByteBuffer.wrap(b);
            bb.putInt(b.length - 4).rewind();
            sendBuffer(bb);
            if (h.getXid() > 0) {
                // check throttling
                if (outstandingRequests.decrementAndGet() < 1 ||
                        zkServer.getInProcess() < outstandingLimit) {
                    enableRecv();
                }
            }
        } catch (Exception e) {
            LOG.warn("Unexpected exception. Destruction averted.", e);
        }
    }

    /*
     * (non-Javadoc)
     *
     * @see org.apache.zookeeper.server.ServerCnxnIface#process(org.apache.zookeeper.proto.WatcherEvent)
     */
    @Override
    public void process(WatchedEvent event) {
        //1.在请求头中标记"-1",表明当前是一个通知
        ReplyHeader h = new ReplyHeader(-1, -1L, 0);
        if (LOG.isTraceEnabled()) {
            ZooTrace.logTraceMessage(LOG, ZooTrace.EVENT_DELIVERY_TRACE_MASK,
                    "Deliver event " + event + " to 0x"
                            + Long.toHexString(this.sessionId)
                            + " through " + this);
        }

        // Convert WatchedEvent to a type that can be sent over the wire
        //将WatchedEvent包装成WatcherEvent,便于在网络中传输序列化
        WatcherEvent e = event.getWrapper();
        //向客户端发送通知
        sendResponse(h, e, "notification");
    }

    /*
     * (non-Javadoc)
     *
     * @see org.apache.zookeeper.server.ServerCnxnIface#getSessionId()
     */
    @Override
    public long getSessionId() {
        return sessionId;
    }

    @Override
    public void setSessionId(long sessionId) {
        this.sessionId = sessionId;
        factory.addSession(sessionId, this);
    }

    @Override
    public void setSessionTimeout(int sessionTimeout) {
        this.sessionTimeout = sessionTimeout;
        factory.touchCnxn(this);
    }

    @Override
    public int getInterestOps() {
        if (!isSelectable()) {
            return 0;
        }
        int interestOps = 0;
        if (getReadInterest()) {
            interestOps |= SelectionKey.OP_READ;
        }
        if (getWriteInterest()) {
            interestOps |= SelectionKey.OP_WRITE;
        }
        return interestOps;
    }

    @Override
    public InetSocketAddress getRemoteSocketAddress() {
        if (sock.isOpen() == false) {
            return null;
        }
        return (InetSocketAddress) sock.socket().getRemoteSocketAddress();
    }

    public InetAddress getSocketAddress() {
        if (sock.isOpen() == false) {
            return null;
        }
        return sock.socket().getInetAddress();
    }

    @Override
    protected ServerStats serverStats() {
        if (zkServer == null) {
            return null;
        }
        return zkServer.serverStats();
    }

    @Override
    public boolean isSecure() {
        return false;
    }

    @Override
    public Certificate[] getClientCertificateChain() {
        throw new UnsupportedOperationException(
                "SSL is unsupported in NIOServerCnxn");
    }

    @Override
    public void setClientCertificateChain(Certificate[] chain) {
        throw new UnsupportedOperationException(
                "SSL is unsupported in NIOServerCnxn");
    }


    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        NIOServerCnxn that = (NIOServerCnxn) o;
        return initialized == that.initialized &&
                sessionTimeout == that.sessionTimeout &&
                sessionId == that.sessionId &&
                outstandingLimit == that.outstandingLimit &&
                Objects.equals(factory, that.factory) &&
                Objects.equals(sock, that.sock) &&
                Objects.equals(selectorThread, that.selectorThread) &&
                Objects.equals(sk, that.sk) &&
                Objects.equals(lenBuffer, that.lenBuffer) &&
                Objects.equals(incomingBuffer, that.incomingBuffer) &&
                Objects.equals(outgoingBuffers, that.outgoingBuffers) &&
                Objects.equals(zkServer, that.zkServer) &&
                Objects.equals(outstandingRequests, that.outstandingRequests) &&
                Objects.equals(selectable, that.selectable) &&
                Objects.equals(throttled, that.throttled);
    }

    @Override
    public int hashCode() {
        return Objects.hash(factory, sock, selectorThread, sk, initialized, lenBuffer, incomingBuffer, outgoingBuffers, sessionTimeout, zkServer, outstandingRequests, sessionId, outstandingLimit, selectable, throttled);
    }
}
