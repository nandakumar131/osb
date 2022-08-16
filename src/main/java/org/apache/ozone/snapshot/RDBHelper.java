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

import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

import java.util.ArrayList;
import java.util.List;

public class RDBHelper {

  final RocksDB rocksDB;
  final List<ColumnFamilyDescriptor> columnFamilyDescriptors;
  final List<ColumnFamilyHandle> columnFamilyHandles;

  public RDBHelper(String dbLocation) throws Exception {
    this.columnFamilyDescriptors = new ArrayList<>();
    this.columnFamilyHandles = new ArrayList<>();
    columnFamilyDescriptors.add(new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY));
    columnFamilyDescriptors.add(new ColumnFamilyDescriptor("keyTable".getBytes()));
    this.rocksDB = RocksDB.openReadOnly(dbLocation, columnFamilyDescriptors, columnFamilyHandles);

  }

  public long getKeyCount() throws RocksDBException {

    return rocksDB.getLongProperty(columnFamilyHandles.get(1), "rocksdb.estimate-num-keys");
  }

  public void close() throws Exception {
    rocksDB.close();
  }

}
