package org.apache.ozone.rocksdb;

import org.rocksdb.*;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.UUID;

public class Scratchpad {

    private String dbPath;
    private List<LiveFileMetaData> fileMetaData;

    public Scratchpad(String dbPath) throws RocksDBException {
        this.dbPath = dbPath;
        try (final Options options = new Options().setCreateIfMissing(true)) {
            try (final RocksDB rocksDB = RocksDB.open(options, dbPath)) {
                fileMetaData = rocksDB.getLiveFilesMetaData();
            }
        }
    }

    public void generateData() throws RocksDBException {
        try (final Options options = new Options().setCreateIfMissing(true)) {
            try (final RocksDB rocksDB = RocksDB.open(options, dbPath)) {
                for(int i = 0; i < 1000000; i++) {
                    byte[] key = ("key-" + i).getBytes(StandardCharsets.UTF_8);
                    rocksDB.put(key, UUID.randomUUID().toString().getBytes(StandardCharsets.UTF_8));
                }

                rocksDB.flushWal(true);
            }
        }
    }

    public void printSstFileNames() {
        for (LiveFileMetaData data : fileMetaData) {
            System.out.println("File Name: " + data.fileName());
        }
    }

    public void printFileSize() {
        for (LiveFileMetaData data : fileMetaData) {
            System.out.println("File size (" + data.fileName() + ") : " + data.numEntries());
        }
    }

    public void dumpFiles() throws RocksDBException {
        for (LiveFileMetaData data : fileMetaData) {
            System.out.println("File Data (" + data.fileName() + "):");
            String file = data.path() + data.fileName();
            SstFileReader sstFileReader = new SstFileReader(new Options());
            sstFileReader.open(file);
            System.out.println(new String(sstFileReader.getTableProperties().getColumnFamilyName()));

            SstFileReaderIterator iterator = sstFileReader.newIterator(new ReadOptions());
            iterator.seekToFirst();
            while (iterator.isValid()) {
                System.out.println("key : " + new String(iterator.key()));
                System.out.println("value : " + new String(iterator.value()));
                iterator.next();
            }

        }
    }

    public static void main(String[] args) throws RocksDBException {
        Scratchpad rdbUtil = new Scratchpad("/Users/nvadivelu/Downloads/om/om.db");
        rdbUtil.printSstFileNames();
        rdbUtil.printFileSize();
        //rdbUtil.dumpFiles();
    }
}
