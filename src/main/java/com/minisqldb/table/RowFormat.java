package com.minisqldb.table;

import com.minisqldb.catalog.Catalog;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public final class RowFormat {
    private RowFormat() {}

    public static byte[] serialize(Catalog.TableDef def, Map<String,Object> values, Charset enc) {
        int size = 0;
        List<Object> ordered = new ArrayList<>();
        for (var e : def.columns().entrySet()) {
            String col = e.getKey();
            String type = e.getValue();
            Object v = values.get(col);
            if (v == null) throw new IllegalArgumentException("Missing value for column " + col);
            switch (type) {
                case "INT" -> {
                    int iv = (v instanceof Number) ? ((Number) v).intValue() : Integer.parseInt(v.toString());
                    ordered.add(Integer.valueOf(iv));
                    size += 4;
                }
                case "VARCHAR" -> {
                    byte[] b = v.toString().getBytes(enc);
                    if (b.length > 65535) throw new IllegalArgumentException("VARCHAR too long: " + b.length);
                    ordered.add(b);
                    size += 4 + b.length;
                }
                default -> throw new IllegalArgumentException("Unsupported type: " + type);
            }
        }
        ByteBuffer buf = ByteBuffer.allocate(size);
        int i = 0;
        for (var e : def.columns().entrySet()) {
            String type = e.getValue();
            Object obj = ordered.get(i++);
            if ("INT".equals(type)) {
                buf.putInt(((Integer) obj).intValue());
            } else {
                byte[] b = (byte[]) obj;
                buf.putInt(b.length);
                buf.put(b);
            }
        }
        return buf.array();
    }

    public static Map<String,Object> deserialize(Catalog.TableDef def, byte[] bytes, Charset enc) {
        ByteBuffer buf = ByteBuffer.wrap(bytes);
        LinkedHashMap<String,Object> out = new LinkedHashMap<>();
        for (var e : def.columns().entrySet()) {
            String col = e.getKey();
            String type = e.getValue();
            if ("INT".equals(type)) {
                out.put(col, buf.getInt());
            } else if ("VARCHAR".equals(type)) {
                int len = buf.getInt();
                byte[] b = new byte[len];
                buf.get(b);
                out.put(col, new String(b, enc));
            } else {
                throw new IllegalArgumentException("Unsupported type: " + type);
            }
        }
        return out;
    }
}
