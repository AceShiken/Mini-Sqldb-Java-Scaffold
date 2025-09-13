package com.minisqldb.config;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;


public final class DatabaseConfig {
    public Path dataDir;


    // Page & Buffering
    public int pageSizeBytes = 8192; // 8 KiB
    public int bufferPoolPages = 4096; // ~32 MiB at 8 KiB


    // WAL/Recovery
    public long walSegmentSizeBytes = 64L << 20; // 64 MiB
    public long checkpointIntervalBytes = 128L << 20; // 128 MiB
    public int checkpointIntervalSeconds = 60;
    public enum FsyncMode { ALWAYS, BATCHED, NEVER }
    public FsyncMode walFsyncMode = FsyncMode.ALWAYS;
    public boolean commitFsync = true;


    // Concurrency
    public int maxReaders = 64;
    public boolean singleWriter = true;
    public enum Isolation { READ_COMMITTED }
    public Isolation isolationLevel = Isolation.READ_COMMITTED;


    // SQL/Types
    public Charset stringEncoding = StandardCharsets.UTF_8;
    public int maxVarCharBytes = 65535;


    // Diagnostics
    public boolean enableStats = true;
    public String logLevel = "INFO";
}
