package com.minisqldb.sql;

import com.minisqldb.catalog.Catalog;

import java.util.*;

/**
 * Tiny parser for: CREATE TABLE and single-row INSERT with explicit column list.
 */
public final class SqlMiniParser {
    public record CreateTable(String name, LinkedHashMap<String,String> columns, boolean ifNotExists) {}
    public record InsertInto(String table, List<String> columns, List<String> values) {}
    public record Select(String table, String whereCol, String whereVal) {}


    public static boolean isCreateTable(String sql) { return sql.trim().toUpperCase(Locale.ROOT).startsWith("CREATE TABLE"); }
    public static boolean isInsert(String sql) { return sql.trim().toUpperCase(Locale.ROOT).startsWith("INSERT INTO"); }
    public static boolean isSelect(String sql) { return sql.trim().toUpperCase(Locale.ROOT).startsWith("SELECT"); }


    public static CreateTable parseCreateTable(String sql) {
        String s = sql.trim();
        String up = s.toUpperCase(Locale.ROOT);
        int startName;
        boolean ifNotExists = false;
        if (up.startsWith("CREATE TABLE IF NOT EXISTS")) {
            startName = "CREATE TABLE IF NOT EXISTS".length();
            ifNotExists = true;
        } else {
            startName = "CREATE TABLE".length();
        }
        int paren = s.indexOf('(');
        String name = s.substring(startName, paren).trim();
        int end = s.lastIndexOf(')');
        String inside = s.substring(paren + 1, end).trim();
        LinkedHashMap<String,String> cols = new LinkedHashMap<>();
        if (!inside.isEmpty()) {
            for (String part : inside.split(",")) {
                String[] kv = part.trim().split("\\s+", 2);
                cols.put(kv[0].trim(), kv[1].trim().toUpperCase(Locale.ROOT));
            }
        }
        return new CreateTable(name, cols, ifNotExists);
    }


    public static InsertInto parseInsert(String sql) {
        String up = sql.toUpperCase(Locale.ROOT);
        int intoIdx = up.indexOf("INTO");
        int valuesIdx = up.indexOf("VALUES");
        String between = sql.substring(intoIdx + 4, valuesIdx).trim();
        int lpCols = between.indexOf('(');
        int rpCols = between.lastIndexOf(')');
        String table = between.substring(0, lpCols).trim();
        String colsInside = between.substring(lpCols + 1, rpCols);
        List<String> cols = Arrays.stream(colsInside.split(",")).map(String::trim).toList();


        String afterValues = sql.substring(valuesIdx + 6).trim();
        int lp = afterValues.indexOf('(');
        int rp = afterValues.lastIndexOf(')');
        String valsInside = afterValues.substring(lp + 1, rp);
        List<String> vals = Arrays.stream(valsInside.split(",")).map(String::trim).toList();
        return new InsertInto(table, cols, vals);
    }


    public static Select parseSelect(String sql) {
        String up = sql.toUpperCase(Locale.ROOT);
        if (!up.startsWith("SELECT")) throw new IllegalArgumentException();
        // SELECT * FROM table [WHERE col = val]
        String[] parts = up.split("\\s+");
        int fromIdx = Arrays.asList(parts).indexOf("FROM");
        if (fromIdx < 0) throw new IllegalArgumentException("Missing FROM");
        String table = sql.split("FROM")[1].trim().split(" ")[0].trim();
        String whereCol = null, whereVal = null;
        int whereIdx = up.indexOf("WHERE");
        if (whereIdx >= 0) {
            String cond = sql.substring(whereIdx + 5).trim();
            String[] kv = cond.split("=");
            whereCol = kv[0].trim();
            whereVal = kv[1].trim();
            if (whereVal.startsWith("'") && whereVal.endsWith("'")) {
                whereVal = whereVal.substring(1, whereVal.length()-1);
            }
        }
        return new Select(table, whereCol, whereVal);
    }


    public static String describe(Catalog catalog) {
        var sb = new StringBuilder();
        catalog.allTables().forEach((name, def) -> {
            sb.append(name).append("(");
            boolean first = true;
            for (var e : def.columns().entrySet()) {
                if (!first) sb.append(", ");
                sb.append(e.getKey()).append(" ").append(e.getValue());
                first = false;
            }
            sb.append(")\n");
        });
        return sb.toString();
    }
}
