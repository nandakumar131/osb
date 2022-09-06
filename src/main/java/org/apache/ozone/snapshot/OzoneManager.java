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

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;

public class OzoneManager {

  final private OMDB omdb;
  final private SnapshotManager snapshotManager;
  final private OMKeyTableWriter omKeyTableWriter;

  public OzoneManager(String path) throws Exception {
    String dbPath = Paths.get(path, SnapshotConst.PARENT_DIR).toString();
    final File location = new File(dbPath);
    if (!location.exists()) {
      boolean success = location.mkdir();
      if (!success) {
        throw new IOException(
            "Unable to create RocksDB parent directory: " +
                location);
      }
    }

    this.omdb = OMDBFactory.createOMDBWithCompactionListener(dbPath);
    this.snapshotManager = new SnapshotManagerImpl(Paths.get(dbPath, SnapshotConst.CHECKPOINT_LOCATION).toString());
    this.omKeyTableWriter = new OMKeyTableWriter(omdb);
  }

  public OMKeyTableWriter getKeyTableWriter() throws IOException {
    return omKeyTableWriter;
  }

  public OMDB getOMDB() {
    return omdb;
  }

  public SnapshotManager getSnapshotManager() {
    return snapshotManager;
  }

  public void close() throws Exception {
    omKeyTableWriter.close();
    omdb.close();
  }
}
