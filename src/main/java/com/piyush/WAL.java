package com.piyush;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;

/**
 * Write-Ahead Log for durability
 * Format: key|value|timestamp|tombstone\n
 */
public class WAL {
    private final Path walPath;
    private BufferedWriter writer;

    public WAL(Path walPath) throws IOException {
        this.walPath = walPath;
        if (walPath.getParent() != null) {
            Files.createDirectories(walPath.getParent());
        }
        this.writer = new BufferedWriter(new FileWriter(walPath.toFile(), true));
    }

    /**
     * Append an entry to the WAL
     */
    public synchronized void append(Entry entry) throws IOException {
        String line = serialize(entry);
        writer.write(line);
        writer.newLine();
        writer.flush();
    }

    /**
     * Recover entries from WAL after crash
     */
    public List<Entry> recover() throws IOException {
        List<Entry> entries = new ArrayList<>();

        if (!Files.exists(walPath)) {
            return entries;
        }

        try (BufferedReader reader = new BufferedReader(new FileReader(walPath.toFile()))) {
            String line;
            while ((line = reader.readLine()) != null) {
                Entry entry = deserialize(line);
                if (entry != null) {
                    entries.add(entry);
                }
            }
        }

        return entries;
    }

    /**
     * Clear the WAL after successful memtable flush
     */
    public synchronized void clear() throws IOException {
        close();
        Files.deleteIfExists(walPath);
        this.writer = new BufferedWriter(new FileWriter(walPath.toFile(), true));
    }

    /**
     * Close the WAL
     */
    public synchronized void close() throws IOException {
        if (writer != null) {
            writer.close();
        }
    }

    private String serialize(Entry entry) {
        String key = escape(entry.getKey());
        String value = entry.getValue() == null ? "" : escape(entry.getValue());

        return String.format("%s|%s|%d|%b",
                key,
                value,
                entry.getTimestamp(),
                entry.isTombstone());
    }

    private Entry deserialize(String line) {
        try {
            String[] parts = splitByUnescapedPipe(line);
            if (parts.length != 4) {
                return null;
            }

            long timestamp = Long.parseLong(parts[2]);
            boolean tombstone = Boolean.parseBoolean(parts[3]);

            String key = unescape(parts[0]);
            String value = parts[1].isEmpty() && tombstone ? null : unescape(parts[1]);

            return new Entry(key, value, timestamp, tombstone);
        } catch (Exception e) {
            System.err.println("Failed to deserialize WAL entry: " + line);
            return null;
        }
    }

    private String escape(String str) {
        return str.replace("\\", "\\\\").replace("|", "\\|");
    }

    private String unescape(String str) {
        return str.replace("\\|", "|").replace("\\\\", "\\");
    }

    private String[] splitByUnescapedPipe(String line) {
        java.util.List<String> parts = new java.util.ArrayList<>();
        StringBuilder current = new StringBuilder();

        for (int i = 0; i < line.length(); i++) {
            char c = line.charAt(i);

            if (c == '\\' && i + 1 < line.length()) {
                current.append(c);
                current.append(line.charAt(i + 1));
                i++;
            } else if (c == '|') {
                parts.add(current.toString());
                current = new StringBuilder();
            } else {
                current.append(c);
            }
        }

        parts.add(current.toString());
        return parts.toArray(new String[0]);
    }
}