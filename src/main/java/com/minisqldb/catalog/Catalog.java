package com.minisqldb.catalog;

import java.util.LinkedHashMap;
import java.util.Map;


public final class Catalog {
    /**
     * @param columns col -> type
     */
    public record TableDef(String name, LinkedHashMap<String, String> columns) {
    }


    private final Map<String, TableDef> tables = new LinkedHashMap<>();


    public synchronized void createTable(TableDef def) {
        if (tables.containsKey(def.name)) throw new IllegalArgumentException("Table exists: " + def.name);
        tables.put(def.name, def);
    }


    public synchronized TableDef getTable(String name) {
        return tables.get(name);
    }

    public synchronized void dropTable(String name) {
        tables.remove(name);
    }


    public synchronized Map<String, TableDef> allTables() {
        return Map.copyOf(tables);
    }
}
