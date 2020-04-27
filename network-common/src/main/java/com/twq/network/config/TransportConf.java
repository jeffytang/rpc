package com.twq.network.config;

import com.google.common.primitives.Ints;
import com.twq.network.util.JavaUtils;

import java.util.Locale;

public class TransportConf {
    
    private final String SPARK_NETWORK_IO_MODE_KEY = "io.mode";
    private final String SPARK_NETWORK_IO_PREFERDIRECTBUFS_KEY = "io.preferDirectBufs";
    private final String SPARK_NETWORK_IO_CONNECTIONTIMEOUT_KEY = "io.connectionTimeout";
    private final String SPARK_NETWORK_IO_BACKLOG_KEY = "io.backLog";
    private final String SPARK_NETWORK_IO_NUMCONNECTIONSPERPEER_KEY = "io.numConnectionsPerPeer";
    private final String SPARK_NETWORK_IO_SERVERTHREADS_KEY = "io.serverThreads";
    private final String SPARK_NETWORK_IO_CLIENTTHREADS_KEY = "io.clientThreads";
    private final String SPARK_NETWORK_IO_RECEIVEBUFFER_KEY = "io.receiveBuffer";
    private final String SPARK_NETWORK_IO_SENDBUFFER_KEY = "io.sendBuffer";
    private final String SPARK_NETWORK_IO_MAXRETRIES_KEY = "io.maxRetries";
    private final String SPARK_NETWORK_IO_RETRYWAIT_KEY = "io.retryWait";
    private final String SPARK_NETWORK_IO_LAZYFD_KEY = "io.lazyFD";

    private ConfigProvider conf;

    public TransportConf(ConfigProvider conf) {
        this.conf = conf;
    }

    public int getInt(String name, int defaultValue) {
        return conf.getInt(name, defaultValue);
    }

    public String get(String name, String defaultValue) {
        return conf.get(name, defaultValue);
    }

    /** IO mode: nio or epoll */
    public String ioMode() {
        return conf.get(SPARK_NETWORK_IO_MODE_KEY, "NIO").toUpperCase(Locale.ROOT);
    }

    /** If true, we will prefer allocating off-heap byte buffers within Netty. */
    public boolean preferDirectBufs() {
        return conf.getBoolean(SPARK_NETWORK_IO_PREFERDIRECTBUFS_KEY, true);
    }

    /** Connect timeout in milliseconds. Default 120 secs. */
    public int connectionTimeoutMs() {
        long defaultTimeoutMs = JavaUtils.timeStringAsSec(
                conf.get(SPARK_NETWORK_IO_CONNECTIONTIMEOUT_KEY, "120s")) * 1000;
        return (int) defaultTimeoutMs;
    }

    /** Number of concurrent connections between two nodes for fetching data. */
    public int numConnectionsPerPeer() {
        return conf.getInt(SPARK_NETWORK_IO_NUMCONNECTIONSPERPEER_KEY, 1);
    }

    /** Requested maximum length of the queue of incoming connections. Default is 64. */
    public int backLog() { return conf.getInt(SPARK_NETWORK_IO_BACKLOG_KEY, 64); }

    /** Number of threads used in the server thread pool. Default to 0, which is 2x#cores. */
    public int serverThreads() { return conf.getInt(SPARK_NETWORK_IO_SERVERTHREADS_KEY, 0); }

    /** Number of threads used in the client thread pool. Default to 0, which is 2x#cores. */
    public int clientThreads() { return conf.getInt(SPARK_NETWORK_IO_CLIENTTHREADS_KEY, 0); }

    /**
     * Receive buffer size (SO_RCVBUF).
     * Note: the optimal size for receive buffer and send buffer should be
     *  latency * network_bandwidth.
     * Assuming latency = 1ms, network_bandwidth = 10Gbps
     *  buffer size should be ~ 1.25MB
     */
    public int receiveBuf() { return conf.getInt(SPARK_NETWORK_IO_RECEIVEBUFFER_KEY, -1); }

    /** Send buffer size (SO_SNDBUF). */
    public int sendBuf() { return conf.getInt(SPARK_NETWORK_IO_SENDBUFFER_KEY, -1); }

    /**
     * Max number of times we will try IO exceptions (such as connection timeouts) per request.
     * If set to 0, we will not do any retries.
     */
    public int maxIORetries() { return conf.getInt(SPARK_NETWORK_IO_MAXRETRIES_KEY, 3); }

    /**
     * Time (in milliseconds) that we will wait in order to perform a retry after an IOException.
     * Only relevant if maxIORetries &gt; 0.
     */
    public int ioRetryWaitTimeMs() {
        return (int) JavaUtils.timeStringAsSec(conf.get(SPARK_NETWORK_IO_RETRYWAIT_KEY, "5s")) * 1000;
    }

    /**
     * Whether to initialize FileDescriptor lazily or not. If true, file descriptors are
     * created only when data is going to be transferred. This can reduce the number of open files.
     */
    public boolean lazyFileDescriptor() {
        return conf.getBoolean(SPARK_NETWORK_IO_LAZYFD_KEY, true);
    }

    /**
     * Minimum size of a block that we should start using memory map rather than reading in through
     * normal IO operations. This prevents Spark from memory mapping very small blocks. In general,
     * memory mapping has high overhead for blocks close to or below the page size of the OS.
     */
    public int memoryMapBytes() {
        return Ints.checkedCast(JavaUtils.byteStringAsBytes(
                conf.get("io.memoryMapThreshold", "2m")));
    }
}
