/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.ozone.snapshot;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.utils.db.DBProfile;
import org.apache.hadoop.hdds.utils.db.DBStore;
import org.apache.hadoop.hdds.utils.db.DBStoreBuilder;
import org.apache.hadoop.hdds.utils.db.RocksDBConfiguration;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.ozone.om.OmMetadataManagerImpl;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.rocksdb.DBOptions;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;

import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_DB_PROFILE;
import static org.apache.hadoop.hdds.utils.db.DBStoreBuilder.HDDS_DEFAULT_DB_PROFILE;
import static org.apache.hadoop.ozone.om.OmMetadataManagerImpl.KEY_TABLE;

public class OMDBReadOnly implements OMDB {

  private OzoneConfiguration configuration;
  private DBStore dbStore;

  public OMDBReadOnly(String dbLocation, String dbName) throws IOException {
    this.configuration = getOzoneConfiguration();
    this.dbStore = getDBStore(new File(dbLocation), dbName);

  }

  private DBStore getDBStore(File metaDir, String dbName) throws IOException {
    RocksDBConfiguration rocksDBConfiguration =
        configuration.getObject(RocksDBConfiguration.class);

    DBProfile dbProfile = configuration.getEnum(HDDS_DB_PROFILE,
        HDDS_DEFAULT_DB_PROFILE);

    DBOptions dBOptions = dbProfile.getDBOptions();

    DBStoreBuilder dbStoreBuilder = DBStoreBuilder.newBuilder(configuration,
            rocksDBConfiguration)
        .setPath(Paths.get(metaDir.getPath()))
        .setName(dbName);

    dbStoreBuilder.setOpenReadOnly(true);
    dbStoreBuilder.setDBOptions(dBOptions);
    return OmMetadataManagerImpl.addOMTablesAndCodecs(dbStoreBuilder).build();
  }
  @Override
  public Table<String, OmKeyInfo> getKeyTable() throws IOException {
    return dbStore.getTable(KEY_TABLE, String.class, OmKeyInfo.class);
  }

  @Override
  public void close() throws Exception {
    dbStore.close();
  }
}
