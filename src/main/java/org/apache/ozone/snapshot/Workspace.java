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

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

public class Workspace {

    private static final AtomicBoolean wasCbCalled = new AtomicBoolean();
    private static final String dbPath   = "./rocksdb-data/";
    private static final String cpPath1   = "./rocksdb-data-1/";
    private static final String cpPath2   = "./rocksdb-data-2/";
    private static final String cfdbPath = "./rocksdb-data-cf/";
    private static final int NUM_ROW = 100_000_000;

    static {
        RocksDB.loadLibrary();
    }

    //  RocksDB.DEFAULT_COLUMN_FAMILY
    public void testDefaultColumnFamilyOriginal() {
        System.out.println("testDefaultColumnFamily begin...");
        //
        try (final Options options = new Options().setCreateIfMissing(true)) {
            try (final RocksDB rocksDB = RocksDB.open(options, dbPath)) {
                // key-value
                byte[] key = "Hello".getBytes();
                rocksDB.put(key, "World".getBytes());

                System.out.println(new String(rocksDB.get(key)));

                rocksDB.put("SecondKey".getBytes(), "SecondValue".getBytes());

                // List
                List<byte[]> keys = Arrays.asList(key, "SecondKey".getBytes(), "missKey".getBytes());
                List<byte[]> values = rocksDB.multiGetAsList(keys);
                for (int i = 0; i < keys.size(); i++) {
                    System.out.println("multiGet " + new String(keys.get(i)) + ":" + (values.get(i) != null ? new String(values.get(i)) : null));
                }

                // [key - value]
                RocksIterator iter = rocksDB.newIterator();
                for (iter.seekToFirst(); iter.isValid(); iter.next()) {
                    System.out.println("iterator key:" + new String(iter.key()) + ", iter value:" + new String(iter.value()));
                }

                // key
                rocksDB.delete(key);
                System.out.println("after remove key:" + new String(key));

                iter = rocksDB.newIterator();
                for (iter.seekToFirst(); iter.isValid(); iter.next()) {
                    System.out.println("iterator key:" + new String(iter.key()) + ", iter value:" + new String(iter.value()));
                }
            }
        } catch (RocksDBException e) {
            e.printStackTrace();
        }
    }

    public void CreateCheckPoint(String dbPathArg, String cpPathArg,
                                 RocksDB rocksDB) {
        System.out.println("Creating Checkpoint for RocksDB instance : " +
            dbPathArg + "in a CheckPoint Location" + cpPathArg);
        try {
            rocksDB.flush(new FlushOptions());
            Checkpoint cp = Checkpoint.create(rocksDB);
            cp.createCheckpoint(cpPathArg);
        } catch (RocksDBException e) {
            throw new RuntimeException(e.getMessage());
        }
    }

    public void ReadRocksDBInstance(String dbPathArg, RocksDB rocksDB,
                                    FileWriter f) {
        System.out.println("Reading RocksDB instance at : " + dbPathArg);
        boolean createdDB = false;
        //
        try (final Options options =
                 new Options().setParanoidChecks(true).setCreateIfMissing(true).setCompressionType(CompressionType.NO_COMPRESSION).setForceConsistencyChecks(false)) {
            //options.setTargetFileSizeBase();
            if (rocksDB == null) {
                rocksDB = RocksDB.open(options, dbPathArg);
                createdDB = true;
            }
            //rocksDB.deleteFile("000148.sst");
            List<LiveFileMetaData> liveFileMetaDataList = rocksDB.getLiveFilesMetaData();
            for (LiveFileMetaData m : liveFileMetaDataList) {
                System.out.println("Live File Metadata");
                System.out.println("\tFile :" + m.fileName());
                System.out.println("\tLevel :" + m.level());
                System.out.println("\ttable :" + new String(m.columnFamilyName()));
                System.out.println("\tKey Range :" + new String(m.smallestKey()) + " " +
                    "<->" + new String(m.largestKey()));
            }
            RocksIterator iter = rocksDB.newIterator();
            for (iter.seekToFirst(); iter.isValid(); iter.next()) {
                //System.out.println("iterator key:" + new String(iter.key()) + ", " +
                //   "iter value:" + new String(iter.value()));
                //f.write("iterator key:" + new String(iter.key()) + ", iter " +
                //    "value:" + new String(iter.value()));
                //f.write("\n");
            }
        } catch (RocksDBException e) {
            e.printStackTrace();
        } finally {
            if (createdDB){
                rocksDB.close();
            }
        }
    }

    //  RocksDB.DEFAULT_COLUMN_FAMILY
    public RocksDB CreateRocksDBInstance(String dbPathArg) {
        System.out.println("Creating RocksDB instance at :" + dbPathArg);
        RocksDB rocksDB = null;
        //
        try (final Options options =
                 new Options().setCreateIfMissing(true).setCompressionType(CompressionType.NO_COMPRESSION)) {
            options.setDisableAutoCompactions(true);
            rocksDB = RocksDB.open(options, dbPathArg);
            // key-value
            for (int i = 0; i< NUM_ROW; ++i) {
                byte[] array = new byte[7]; // length is bounded by 7
                new Random().nextBytes(array);
                String keyStr = " My" + array + "StringKey" + i;
                String valueStr = " My " + array + "StringValue" + i;
                byte[] key = keyStr.getBytes();
                rocksDB.put(keyStr.getBytes(StandardCharsets.UTF_8), valueStr.getBytes(StandardCharsets.UTF_8));
                //System.out.println(new String(rocksDB.get(key)));
            }
        } catch (RocksDBException e) {
            e.printStackTrace();
        }
        return rocksDB;
    }

    //  RocksDB.DEFAULT_COLUMN_FAMILY
    public void UpdateRocksDBInstance(String dbPathArg, RocksDB rocksDB) {
        System.out.println("Updating RocksDB instance at :" + dbPathArg);
        //
        try (final Options options =
                 new Options().setCreateIfMissing(true).setCompressionType(CompressionType.NO_COMPRESSION)) {
            if (rocksDB == null) {
                rocksDB = RocksDB.open(options, dbPathArg);
            }
            // key-value
            for (int i = 0; i< NUM_ROW; ++i) {
                byte[] array = new byte[7]; // length is bounded by 7
                new Random().nextBytes(array);
                String keyStr = " MyUpdated" + array + "StringKey" + i;
                String valueStr = " My Updated" + array + "StringValue" + i;
                byte[] key = keyStr.getBytes();
                rocksDB.put(keyStr.getBytes(StandardCharsets.UTF_8), valueStr.getBytes(StandardCharsets.UTF_8));
                System.out.println(new String(rocksDB.get(key)));
            }
        } catch (RocksDBException e) {
            e.printStackTrace();
        }
    }

    // (table)
    public void testCertainColumnFamily() {
        System.out.println("\ntestCertainColumnFamily begin...");
        try (final ColumnFamilyOptions cfOpts = new ColumnFamilyOptions().optimizeUniversalStyleCompaction()) {
            String cfName = "my-first-columnfamily";
            // list of column family descriptors, first entry must always be default column family
            final List<ColumnFamilyDescriptor> cfDescriptors = Arrays.asList(
                new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, cfOpts),
                new ColumnFamilyDescriptor(cfName.getBytes(), cfOpts)
            );

            List<ColumnFamilyHandle> cfHandles = new ArrayList<>();
            try (final DBOptions dbOptions = new DBOptions().setCreateIfMissing(true).setCreateMissingColumnFamilies(true);
                 final RocksDB rocksDB = RocksDB.open(dbOptions, cfdbPath, cfDescriptors, cfHandles)) {
                ColumnFamilyHandle cfHandle = cfHandles.stream().filter(x -> {
                    try {
                        return (new String(x.getName())).equals(cfName);
                    } catch (RocksDBException e) {
                        return false;
                    }
                }).collect(Collectors.toList()).get(0);

                // key/value
                String key = "FirstKey";
                rocksDB.put(cfHandles.get(0), key.getBytes(), "FirstValue".getBytes());
                // key
                byte[] getValue = rocksDB.get(cfHandles.get(0), key.getBytes());
                System.out.println("get Value : " + new String(getValue));
                // key/value
                rocksDB.put(cfHandles.get(1), "SecondKey".getBytes(),
                    "SecondValue".getBytes());

                List<byte[]> keys = Arrays.asList(key.getBytes(), "SecondKey".getBytes());
                List<ColumnFamilyHandle> cfHandleList = Arrays.asList(cfHandle, cfHandle);
                // key
                List<byte[]> values = rocksDB.multiGetAsList(cfHandleList, keys);
                for (int i = 0; i < keys.size(); i++) {
                    System.out.println("multiGet:" + new String(keys.get(i)) + "--" + (values.get(i) == null ? null : new String(values.get(i))));
                }
                //rocksDB.compactRange();
                //rocksDB.compactFiles();
                List<LiveFileMetaData> liveFileMetaDataList = rocksDB.getLiveFilesMetaData();
                for (LiveFileMetaData m : liveFileMetaDataList) {
                    System.out.println("Live File Metadata");
                    System.out.println("\tFile :" + m.fileName());
                    System.out.println("\ttable :" + new String(m.columnFamilyName()));
                    System.out.println("\tKey Range :" + new String(m.smallestKey()) + " " +
                        "<->" + new String(m.largestKey()));
                }
                // key
                rocksDB.delete(cfHandle, key.getBytes());

                // key
                RocksIterator iter = rocksDB.newIterator(cfHandle);
                for (iter.seekToFirst(); iter.isValid(); iter.next()) {
                    System.out.println("iterator:" + new String(iter.key()) + ":" + new String(iter.value()));
                }
            } finally {
                // NOTE frees the column family handles before freeing the db
                for (final ColumnFamilyHandle cfHandle : cfHandles) {
                    cfHandle.close();
                }
            }
        } catch (RocksDBException e) {
            e.printStackTrace();
        } // frees the column family options
    }

    public static FileWriter createFile(String fileName) throws IOException {
        File file = new File(fileName);
        file.createNewFile();
        return new FileWriter(fileName);
    }

    public static RocksDB setCallbackForCompactionCompleted() throws RocksDBException {
        final AbstractEventListener onCompactionCompletedListener =
            new AbstractEventListener() {
                @Override
                public void onCompactionCompleted(final RocksDB db, final CompactionJobInfo compactionJobInfo) {
                    System.out.println(compactionJobInfo.compactionReason());
                    System.out.println("List of input files:");
                    for (String file : compactionJobInfo.inputFiles()) {
                        System.out.println(file);
                    }
                    System.out.println("List of output files:");
                    for (String file : compactionJobInfo.outputFiles()) {
                        System.out.println(file);
                    }
                    wasCbCalled.set(true);
                }
            };
        final Options opt = new Options().setCreateIfMissing(true)
            .setCompressionType(CompressionType.NO_COMPRESSION)
            .setListeners(Collections
                .singletonList(onCompactionCompletedListener));
        RocksDB db = RocksDB.open(opt, dbPath);
        return  db;
    }


    public static void main(String[] args) throws Exception {
        Workspace test = new Workspace();
        RocksDB rocksDB = test.CreateRocksDBInstance(dbPath);
        rocksDB.close();
        //test.ReadRocksDBInstance(dbPath, rocksDB, createFile("FirstUpdateFile"));
        //test.ReadRocksDBInstance(dbPath, null, createFile("FirstUpdateFile"));
        rocksDB = setCallbackForCompactionCompleted();
        Thread.sleep(10000);

        System.out.println("Current time is::" + System.currentTimeMillis());
        long t1 = System.currentTimeMillis();
        test.CreateCheckPoint(dbPath, cpPath2, rocksDB);

        long t2 = System.currentTimeMillis();
        System.out.println("Current time is::" + t2);
        System.out.println("millisecond difference is ::" + (t2-t1));
        //test.ReadRocksDBInstance(cpPath1, null, createFile("CheckPointFile"));
        //test.UpdateRocksDBInstance(dbPath, rocksDB);
        //test.ReadRocksDBInstance(cpPath1, null, createFile(
        //    "CheckPointFileAfterUpdateToActive"));
        //test.ReadRocksDBInstance(dbPath, null, createFile(
        //    "ActiveFileAfterUpdate"));
        //test.testCertainColumnFamily();
        //test.testCertainColumnFamily();
    }

}