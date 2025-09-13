package com.minisqldb.storage;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.HashMap;
import java.util.Map;

public final class Pager implements AutoCloseable {
    private final Path file;
    private final int pageSize;
    private final FileChannel ch;

    // Tiny cache (not LRU yet)
    private final Map<Integer, Page> cache = new HashMap<>();

    public Pager(Path file, int pageSize) throws IOException {
        this.file = file;
        this.pageSize = pageSize;
        Files.createDirectories(file.getParent());
        this.ch = FileChannel.open(file,
                StandardOpenOption.CREATE,
                StandardOpenOption.READ,
                StandardOpenOption.WRITE);
    }

    public int pageSize() { return pageSize; }

    public synchronized int pageCount() throws IOException {
        long size = ch.size();
        return (int) (size / pageSize);
    }

    public synchronized Page read(int pageId) throws IOException {
        Page cached = cache.get(pageId);
        if (cached != null) return cached;
        ByteBuffer buf = ByteBuffer.allocateDirect(pageSize);
        long pos = (long) pageId * pageSize;
        ByteBuffer tmp = ByteBuffer.allocate(pageSize);
        ch.read(tmp, pos);
        tmp.flip();
        buf.put(tmp);
        buf.clear();
        Page p = new Page(pageId, buf);
        cache.put(pageId, p);
        return p;
    }

    public synchronized void write(Page page) throws IOException {
        ByteBuffer buf = page.getBuf().duplicate();
        buf.rewind();
        long pos = (long) page.getId() * pageSize;
        while (buf.hasRemaining()) ch.write(buf, pos);
        page.markDirty();
    }

    public synchronized int allocateNewPage() throws IOException {
        int nextId = (int) (ch.size() / pageSize);
        Page p = new Page(nextId, ByteBuffer.allocateDirect(pageSize));
        cache.put(nextId, p);
        return nextId;
    }

    public synchronized long sizeBytes() throws IOException {
        return ch.size();
    }

    public synchronized void force() throws IOException {
        ch.force(true);
    }

    @Override
    public void close() throws IOException {
        ch.close();
    }
}