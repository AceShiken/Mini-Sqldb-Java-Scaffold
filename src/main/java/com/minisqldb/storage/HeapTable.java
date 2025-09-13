package com.minisqldb.storage;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.function.Consumer;

/**
 * Append-only heap table over a file of fixed-size pages.
 * Page layout: [int used] then sequence of [int rowLen][rowBytes].
 */
public final class HeapTable implements AutoCloseable {
    private static final int HEADER_USED = 0; // offset of used pointer
    private static final int HEADER_BYTES = 4;


    private final Pager pager;


    public HeapTable(Path file, int pageSize) throws IOException { this.pager = new Pager(file, pageSize); }


    private void initIfNeeded(Page p) {
        ByteBuffer b = p.getBuf();
        int used = b.getInt(HEADER_USED);
        if (used == 0) b.putInt(HEADER_USED, HEADER_BYTES);
    }


    private int used(Page p) {
        int u = p.getBuf().getInt(HEADER_USED);
        return u == 0 ? HEADER_BYTES : u;
    }


    public synchronized void insert(byte[] rowPayload) throws IOException {
        byte[] rec = new byte[4 + rowPayload.length];
        ByteBuffer.wrap(rec).putInt(rowPayload.length).put(rowPayload);


        int pages = pager.pageCount();
        int pid = (pages == 0) ? pager.allocateNewPage() : pages - 1;
        Page page = pager.read(pid);
        initIfNeeded(page);
        int u = used(page);
        int free = pager.pageSize() - u;
        if (rec.length > free) {
            pid = pager.allocateNewPage();
            page = pager.read(pid);
            initIfNeeded(page);
            u = used(page);
        }
        ByteBuffer b = page.getBuf().duplicate();
        b.position(u);
        b.put(rec);
        page.getBuf().putInt(HEADER_USED, u + rec.length);
        pager.write(page);
    }


    public synchronized void forEach(Consumer<byte[]> v) throws IOException {
        int pages = pager.pageCount();
        for (int id = 0; id < pages; id++) {
            Page p = pager.read(id);
            int u = used(p);
            ByteBuffer b = p.getBuf().duplicate();
            int pos = HEADER_BYTES;
            while (pos + 4 <= u) {
                int len = b.getInt(pos);
                if (len < 0 || pos + 4 + len > u) break;
                byte[] rec = new byte[len];
                b.position(pos + 4);
                b.get(rec, 0, len);
                v.accept(rec);
                pos += 4 + len;
            }
        }
    }


    @Override
    public void close() throws IOException {
        pager.close();
    }
}