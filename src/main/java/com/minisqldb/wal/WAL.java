package com.minisqldb.wal;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.time.Instant;
import java.util.Base64;


public final class WAL implements AutoCloseable {
    private final FileChannel binCh; // wal.log (length-prefixed binary)
    private final FileChannel txtCh; // wal.txt (JSON lines)

    public WAL(Path walFile) throws IOException {
        Files.createDirectories(walFile.getParent());
        this.binCh = FileChannel.open(walFile,
                StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE);
        Path txt = walFile.getParent().resolve("wal.txt");
        this.txtCh = FileChannel.open(txt,
                StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE);
    }

    /** Append a raw binary record (length-prefixed). */
    public synchronized void append(byte[] record) throws IOException {
        ByteBuffer len = ByteBuffer.allocate(4).putInt(record.length);
        len.flip();
        binCh.position(binCh.size());
        binCh.write(len);
        binCh.write(ByteBuffer.wrap(record));
    }

    /** Append a human-readable JSONL line. */
    public synchronized void appendText(String jsonLine) throws IOException {
        byte[] b = (jsonLine + "\n").getBytes(StandardCharsets.UTF_8);
        txtCh.position(txtCh.size());
        txtCh.write(ByteBuffer.wrap(b));
    }

    // Convenience helpers
    public void logCreateTable(String name) throws IOException {
        appendText("{\"ts\":\"" + Instant.now() + "\",\"op\":\"CREATE_TABLE\",\"table\":\"" + esc(name) + "\"}");
    }
    public void logDropTable(String name) throws IOException {
        appendText("{\"ts\":\"" + Instant.now() + "\",\"op\":\"DROP_TABLE\",\"table\":\"" + esc(name) + "\"}");
    }
    public void logTruncate(String name) throws IOException {
        appendText("{\"ts\":\"" + Instant.now() + "\",\"op\":\"TRUNCATE\",\"table\":\"" + esc(name) + "\"}");
    }
    public void logInsert(String table, byte[] row) throws IOException {
        String b64 = Base64.getEncoder().encodeToString(row);
        appendText("{\"ts\":\"" + Instant.now() + "\",\"op\":\"INSERT\",\"table\":\"" + esc(table) + "\",\"row\":\"" + b64 + "\"}");
        // minimal binary: [op=1][tableLen][tableUtf8][rowLen][rowBytes]
        byte[] t = table.getBytes(StandardCharsets.UTF_8);
        ByteBuffer buf = ByteBuffer.allocate(1 + 4 + t.length + 4 + row.length);
        buf.put((byte)1).putInt(t.length).put(t).putInt(row.length).put(row);
        append(buf.array());
    }

    public synchronized void sync() throws IOException {
        binCh.force(true);
        txtCh.force(true);
    }
    private static String esc(String s){
        return s.replace("\\", "\\\\").replace("\"","\\\"");
    }
    @Override public void close() throws IOException {
        binCh.close(); txtCh.close();
    }
}