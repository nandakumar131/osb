/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.apache.ozone.snapshot;

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
