package com.minisqldb;


//TIP To <b>Run</b> code, press <shortcut actionId="Run"/> or
// click the <icon src="AllIcons.Actions.Execute"/> icon in the gutter.

import com.minisqldb.config.DatabaseConfig;
import com.minisqldb.sql.SqlMiniParser;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;


public final class Main {
    public static void main(String[] args) throws Exception {
        Path dataDir = Path.of("./data");
        for (int i=0;i<args.length-1;i++) if ("--data".equals(args[i])) dataDir = Path.of(args[i+1]);
        DatabaseConfig cfg = new DatabaseConfig(); cfg.dataDir = dataDir;
        try (Database db = Database.open(cfg)) { repl(db); }
    }


    private static void repl(Database db) throws IOException {
        System.out.println("MiniSQLDB v0.3 – SELECT enabled (.truncate/.drop)");
        BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
        StringBuilder sb = new StringBuilder();
        while (true) {
            System.out.print("db> ");
            String line = br.readLine();
            if (line == null) break;
            line = line.strip();
            if (line.isEmpty()) continue;

            // meta commands
            if (line.startsWith(".")) {
                String[] parts = line.split("\s+", 3);
                switch (parts[0]) {
                    case ".help" -> System.out.println("Commands: .help .quit .tables .dump <table> .truncate <table> .drop <table>");
                    case ".quit" -> { return; }
                    case ".tables" -> System.out.print(SqlMiniParser.describe(db.catalog()));
                    case ".dump" -> {
                        if (parts.length < 2) { System.out.println("Usage: .dump <table>"); break; }
                        try { System.out.print(db.dump(parts[1].trim())); }
                        catch (Throwable t) { System.out.println("Error: " + t.getMessage()); }
                    }
                    case ".truncate" -> {
                        if (parts.length < 2) { System.out.println("Usage: .truncate <table>"); break; }
                        try { db.truncateTable(parts[1].trim()); System.out.println("OK: truncated"); }
                        catch (Throwable t) { System.out.println("Error: " + t.getMessage()); }
                    }
                    case ".drop" -> {
                        if (parts.length < 2) { System.out.println("Usage: .drop <table>"); break; }
                        try { db.dropTable(parts[1].trim()); System.out.println("OK: dropped"); }
                        catch (Throwable t) { System.out.println("Error: " + t.getMessage()); }
                    }
                    case ".wal" -> {
                        int n = 50; // default tail lines
                        if (parts.length >= 2) { try { n = Integer.parseInt(parts[1]); } catch (NumberFormatException ignore) {} }
                        Path walTxt = Path.of("./data/wal.txt"); // adjust if your dataDir varies
                        if (!Files.exists(walTxt)) { System.out.println("(no wal.txt yet)"); break; }
                        var lines = Files.readAllLines(walTxt);
                        int from = Math.max(0, lines.size() - n);
                        for (int i = from; i < lines.size(); i++) System.out.println(lines.get(i));
                    }
                    default -> System.out.println("Unknown command");
                }
                // do NOT accumulate meta commands
                sb.setLength(0);
                continue;
            }


            // Append this line to the statement buffer
            sb.append(line);


            // If there are multiple semicolons, split them to avoid re-executing
            String current = sb.toString();
            int semi;
            while ((semi = current.indexOf(';')) >= 0) {
                String sql = current.substring(0, semi).trim();
                current = current.substring(semi + 1).trim();
                if (!sql.isEmpty()) {
                    try {
                        executeSql(db, sql);
                    } catch (Throwable t) {
                        System.out.println("Error: " + t.getMessage());
                    }
                }
            }
            // save any leftover partial (no trailing semicolon)
            sb.setLength(0);
            if (!current.isEmpty()) sb.append(current);
        }
    }


    private static void executeSql(Database db, String sql) throws IOException {
        if (SqlMiniParser.isCreateTable(sql)) {
            var ct=SqlMiniParser.parseCreateTable(sql);
            try {
                db.createTable(ct.name(), new LinkedHashMap<>(ct.columns()));
                System.out.println("OK: created table "+ct.name());
            } catch (IllegalArgumentException e) {
                if (ct.ifNotExists()) {
                    System.out.println("Notice: table "+ct.name()+" already exists");
                } else {
                    throw e;
                }
            }
            return;
        }
        if (SqlMiniParser.isInsert(sql)) {
            var ins=SqlMiniParser.parseInsert(sql);
            var def=db.catalog().getTable(ins.table());
            Map<String,Object> row=new HashMap<>();
            for (int i=0;i<ins.columns().size();i++) {
                String col=ins.columns().get(i);
                String raw=ins.values().get(i);
                if (raw.startsWith("'")&&raw.endsWith("'")) row.put(col, raw.substring(1,raw.length()-1));
                else row.put(col,Integer.parseInt(raw));
            }
            db.insertRow(ins.table(), row);
            System.out.println("OK: 1 row inserted");
            return;
        }
        if (SqlMiniParser.isSelect(sql)) {
            var sel=SqlMiniParser.parseSelect(sql);
            String result=db.select(sel);
            System.out.print(result);
            return;
        }
        System.out.println("Unknown SQL");
    }
}