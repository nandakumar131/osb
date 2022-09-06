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

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.utils.db.DBCheckpoint;
import org.apache.hadoop.hdds.utils.db.RDBStore;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OmMetadataManagerImpl;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;

import java.io.IOException;

public class OMDBImpl implements OMDB {

  private OMMetadataManager omMetadataManager;
  private OzoneConfiguration configuration;


  public OMDBImpl(String dbLocation) throws IOException {
    this.configuration = getOzoneConfiguration();
    this.configuration.set(OMConfigKeys.OZONE_OM_DB_DIRS, dbLocation);
    this.omMetadataManager = new OmMetadataManagerImpl(configuration);
  }

  @Override
  public Table<String, OmKeyInfo> getKeyTable() {
    return omMetadataManager.getKeyTable(BucketLayout.OBJECT_STORE);
  }

  @Override
  public DBCheckpoint createSnapshot(String name) throws IOException {
    final RDBStore store = (RDBStore) omMetadataManager.getStore();
    return store.getSnapshot(name);
  }

  @Override
  public void close() throws Exception {
    omMetadataManager.stop();
  }
}
