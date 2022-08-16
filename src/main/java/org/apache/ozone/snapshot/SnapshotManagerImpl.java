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

import java.util.Collections;
import java.util.List;

public class SnapshotManagerImpl implements SnapshotManager {

  private final String checkpointParentDir;
  private final SnapshotDBHandler snapshotDBHandler;
  private final SnapDiffManager snapshotDiffManager;

  public SnapshotManagerImpl(String checkpointParentDir) {
    this.checkpointParentDir = checkpointParentDir;
    this.snapshotDBHandler = new SnapshotDBHandler(checkpointParentDir);
    this.snapshotDiffManager = new SnapDiffManager(snapshotDBHandler);
  }

  public List<String> getAllSnapshots(final String volume, final String bucket) {
    // TODO: Implement the logic for getting all snapshots.
    return Collections.emptyList();
  }

  public OMDB getOMDB(final String snapshotName)
      throws Exception {
    return snapshotDBHandler.getOMDB(snapshotName);
  }

  public SnapDiffManager getSnapshotDiffManager() {
    return snapshotDiffManager;
  }


}
