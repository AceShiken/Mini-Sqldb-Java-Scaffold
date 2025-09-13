package com.minisqldb.storage;

import lombok.Data;

import java.nio.ByteBuffer;

@Data
public final class Page {
    public static final int META_PAGE_ID = 0; // reserved

    private final int id;
    private final ByteBuffer buf;
    private boolean dirty;

    public Page(int id, ByteBuffer buf) {
        this.id = id;
        this.buf = buf;
        this.dirty = false;
    }
    public void markDirty() {
        this.dirty = true;
    }
}