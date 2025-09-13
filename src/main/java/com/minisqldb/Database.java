package com.minisqldb;

import com.minisqldb.catalog.Catalog;
import com.minisqldb.config.DatabaseConfig;
import com.minisqldb.sql.SqlMiniParser;
import com.minisqldb.storage.HeapTable;
import com.minisqldb.table.RowFormat;
import com.minisqldb.wal.WAL;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;


public final class Database implements AutoCloseable {
    private final DatabaseConfig cfg;
    private final Catalog catalog;
    private final WAL wal; // reserved for future

    private final Path tablesDir;
    private final Map<String, HeapTable> openTables = new HashMap<>();

    private Database(DatabaseConfig cfg, Catalog catalog, WAL wal, Path tablesDir) {
        this.cfg = cfg;
        this.catalog = catalog;
        this.wal = wal;
        this.tablesDir = tablesDir;
    }

    public static Database open(DatabaseConfig cfg) throws IOException {
        if (cfg.dataDir == null) throw new IllegalArgumentException("dataDir must be set");
        Files.createDirectories(cfg.dataDir);
        Path walFile = cfg.dataDir.resolve("wal.log");
        Path tablesDir = cfg.dataDir.resolve("tables");
        Files.createDirectories(tablesDir);

        Catalog catalog = new Catalog();
        WAL wal = new WAL(walFile);
        return new Database(cfg, catalog, wal, tablesDir);
    }

    public Catalog catalog() {
        return catalog;
    }

    private HeapTable ensureTableOpened(String name) {
        try {
            return openTables.computeIfAbsent(name, t -> {
                try {
                    Path file = tablesDir.resolve(t + ".tbl");
                    return new HeapTable(file, cfg.pageSizeBytes);
                } catch (IOException e) { throw new RuntimeException(e); }
            });
        } catch (RuntimeException re) {
            if (re.getCause() instanceof IOException io) {
                throw new IllegalStateException("Failed to open table file: " + name + ": " + io.getMessage(), io);
            }
            throw re;
        }
    }

    public void createTable(String name, LinkedHashMap<String,String> columns) throws IOException {
        if (catalog.getTable(name) != null) throw new IllegalArgumentException("Table exists: " + name);
        catalog.createTable(new Catalog.TableDef(name, new LinkedHashMap<>(columns)));
        Path file = tablesDir.resolve(name + ".tbl");
        if (!Files.exists(file)) Files.createFile(file);
        wal.logCreateTable(name);
        wal.sync();
    }

    public void truncateTable(String name) throws IOException {
        Path file = tablesDir.resolve(name + ".tbl");
        if (Files.exists(file)) {
            try (var ch = FileChannel.open(file, StandardOpenOption.WRITE)) { ch.truncate(0); }
        }
        openTables.remove(name);
        wal.logTruncate(name);
        wal.sync();
    }


    public void dropTable(String name) throws IOException {
        truncateTable(name);
        Files.deleteIfExists(tablesDir.resolve(name + ".tbl"));
        catalog.dropTable(name);
        wal.logDropTable(name);
        wal.sync();
    }

    public void insertRow(String table, Map<String,Object> values) throws IOException {
        Catalog.TableDef def = catalog.getTable(table);
        if (def == null) throw new IllegalArgumentException("No such table: " + table);
        byte[] rec = RowFormat.serialize(def, values, cfg.stringEncoding);
        HeapTable ht = ensureTableOpened(table);
        ht.insert(rec);
    }

    public String select(SqlMiniParser.Select sel) throws IOException {
        var def = catalog.getTable(sel.table());
        if (def==null) throw new IllegalArgumentException("No such table: "+sel.table());
        StringBuilder sb=new StringBuilder();
        ensureTableOpened(sel.table()).forEach(rec->{
            var row=RowFormat.deserialize(def,rec,cfg.stringEncoding);
            if (sel.whereCol()==null) {
                sb.append(row).append("\n");
            } else {
                Object val=row.get(sel.whereCol());
                if (val!=null && val.toString().equals(sel.whereVal())) {
                    sb.append(row).append("\n");
                }
            }
        });
        return sb.toString();
    }

    public String dump(String table) throws IOException {
        Catalog.TableDef def = catalog.getTable(table);
        if (def == null) throw new IllegalArgumentException("No such table: " + table);
        HeapTable ht = ensureTableOpened(table);
        StringBuilder sb = new StringBuilder();
        ht.forEach(rec -> sb.append(RowFormat.deserialize(def, rec, cfg.stringEncoding)).append('\n'));
        return sb.toString();
    }

    public void checkpoint() throws IOException {
        wal.sync();
    }

    @Override public void close() throws IOException {
        checkpoint();
        for (var t : openTables.values()) t.close();
        wal.close();
    }
}