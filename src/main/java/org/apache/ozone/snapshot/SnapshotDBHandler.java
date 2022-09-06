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

import org.apache.hadoop.ozone.om.OmMetadataManagerImpl;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.DBOptions;
import org.rocksdb.LiveFileMetaData;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public class SnapshotDBHandler {

  private final String checkpointParentDir;

  // Introduce DB instance cache to avoid creating DB instance for every operation.

  public SnapshotDBHandler(String checkpointParentDir) {
    this.checkpointParentDir = checkpointParentDir;
  }

  public long getKeyCount(String snapshotName) throws Exception {
    final RDBHelper rdbHelper = new RDBHelper(checkpointParentDir + "/" + snapshotName);
    long count = rdbHelper.getKeyCount();
    rdbHelper.close();
    return count;
  }

  public OMDB getOMDB(String snapshotName)
      throws Exception {
    return OMDBFactory.createOMDBReadeOnly(checkpointParentDir, snapshotName);
  }

  public List<LiveFileMetaData> getKeyTableSSTFiles(String snapshotName) throws RocksDBException {
    final String dbLocation = checkpointParentDir + "/" + snapshotName;
    final List<ColumnFamilyHandle> columnFamilyHandles  = new ArrayList<>();
    final List<ColumnFamilyDescriptor> columnFamilyDescriptors = new ArrayList<>();
    columnFamilyDescriptors.add(
        new ColumnFamilyDescriptor(
            OmMetadataManagerImpl.KEY_TABLE.getBytes(
                StandardCharsets.UTF_8)));
    columnFamilyDescriptors.add(
        new ColumnFamilyDescriptor(
            "default".getBytes(
                StandardCharsets.UTF_8)));
    try (final DBOptions options = new DBOptions()) {
      try (final RocksDB rocksDB = RocksDB.openReadOnly(options, dbLocation,
          columnFamilyDescriptors, columnFamilyHandles)) {
        return rocksDB.getLiveFilesMetaData();
      }
    }
  }
}
