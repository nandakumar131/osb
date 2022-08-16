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

import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.rocksdb.LiveFileMetaData;
import org.rocksdb.RocksDBException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.ozone.snapshot.OMSSTFileReader.ClosableIterator;

public class SnapDiffManager {

  private final SnapshotDBHandler snapshotDBHandler;

  public SnapDiffManager(SnapshotDBHandler snapshotDBHandler) {
    this.snapshotDBHandler = snapshotDBHandler;
  }


  public List<String> getSnapshotDiff(final String volumeName,
                                      final String BucketName,
                                      final String oldSnapshot,
                                      final String newSnapshot)
      throws Exception {
    // TODO: Make sure that both snapshot belongs to the same volume and bucket.

    final OMDB oldOMDb = snapshotDBHandler.getOMDB(oldSnapshot);
    // TODO: Validate volumeName and bucketName

    final OMDB newOMDb = snapshotDBHandler.getOMDB(newSnapshot);
    // TODO: Validate volumeName and bucketName

    // TODO: Verify if the old snapshot creation time is older than the
    //  new snapshot creation time.

    // Keys that might have changed.
    final ClosableIterator<String> keysToCheck =
        getsKeysToCheck(oldSnapshot, newSnapshot);

    // Open RocksDB and check for the keys.
    final Table<String, OmKeyInfo> oldKeyTable = oldOMDb.getKeyTable();
    final Table<String, OmKeyInfo> newKeyTable = newOMDb.getKeyTable();

    final List<String> deleteDiffs = new ArrayList<>();
    final List<String> renameDiffs = new ArrayList<>();
    final List<String> createDiffs = new ArrayList<>();
    final List<String> modifyDiffs = new ArrayList<>();

    /*
     * The reason for having ObjectID to KeyName mapping instead of OmKeyInfo
     * is to reduce the memory footprint.
     * This will introduce additional DB lookup later on, which should be ok.
     */
    final Map<Long, String> oldObjIdToKeyMap = new HashMap<>();
    final Map<Long, String> newObjIdToKeyMap = new HashMap<>();

    final Set<Long> filteredObjectIDsToCheck = new HashSet<>();

    while(keysToCheck.hasNext()) {
      final String key = keysToCheck.next();
      // TODO: Replace this with multiGet.
      final OmKeyInfo oldKey = oldKeyTable.get(key);
      final OmKeyInfo newKey = newKeyTable.get(key);
      if(areKeysEqual(oldKey, newKey)) {
        // We don't have to do anything.
        continue;
      }
      if (oldKey != null) {
        final long oldObjId = oldKey.getObjectID();
        oldObjIdToKeyMap.put(oldObjId, oldKey.getKeyName());
        filteredObjectIDsToCheck.add(oldObjId);
      }
      if (newKey != null) {
        final long newObjId = newKey.getObjectID();
        newObjIdToKeyMap.put(newObjId, newKey.getKeyName());
        filteredObjectIDsToCheck.add(newObjId);
      }
    }
    keysToCheck.close();

    for (Long id : filteredObjectIDsToCheck) {
      /*
       * This key can be
       * -> Created after the old snapshot was taken, which means it will be
       *    missing in oldKeyTable and present in newKeyTable.
       * -> Deleted after the old snapshot was taken, which means it will be
       *    present in oldKeyTable and missing in newKeyTable.
       * -> Modified after the old snapshot was taken, which means it will be
       *    present in oldKeyTable and present in newKeyTable with same
       *    Object ID but with different metadata.
       * -> Renamed after the old snapshot was taken, which means it will be
       *    present in oldKeyTable and present in newKeyTable but with different
       *    name and same Object ID.
       */

      final String oldKeyName = oldObjIdToKeyMap.get(id);
      final String newKeyName = newObjIdToKeyMap.get(id);

      if (oldKeyName == null && newKeyName == null) {
        // This cannot happen.
        continue;
      }

      // Key Created.
      if (oldKeyName == null) {
        createDiffs.add("+ " + newKeyName);
        continue;
      }

      // Key Deleted.
      if(newKeyName == null) {
        deleteDiffs.add("- " + oldKeyName);
        continue;
      }

      // Key modified.
      if(oldKeyName.equals(newKeyName)) {
        modifyDiffs.add("M " + newKeyName);
        continue;
      }

      // Key Renamed.
      renameDiffs.add("R " + oldKeyName + " -> " + newKeyName);
    }
    oldKeyTable.close();
    newKeyTable.close();
    oldOMDb.close();
    newOMDb.close();

    /*
     * The order in which snap-diff should be applied
     *
     *     1. Delete diffs
     *     2. Rename diffs
     *     3. Create diffs
     *     4. Modified diffs
     *
     * Consider the following scenario
     *
     *    1. File "A" is created.
     *    2. File "B" is created.
     *    3. File "C" is created.
     *    Snapshot "1" is taken.
     *
     * Case 1:
     *   1. File "A" is deleted.
     *   2. File "B" is renamed to "A".
     *   Snapshot "2" is taken.
     *
     *   Snapshot diff should be applied in the following order:
     *    1. Delete "A"
     *    2. Rename "B" to "A"
     *
     *
     * Case 2:
     *    1. File "B" is renamed to "C".
     *    2. File "B" is created.
     *    Snapshot "2" is taken.
     *
     *   Snapshot diff should be applied in the following order:
     *    1. Rename "B" to "C"
     *    2. Create "B"
     *
     */

    final List<String> snapshotDiffs = new ArrayList<>();
    snapshotDiffs.addAll(deleteDiffs);
    snapshotDiffs.addAll(renameDiffs);
    snapshotDiffs.addAll(createDiffs);
    snapshotDiffs.addAll(modifyDiffs);
    return snapshotDiffs;

  }

  private boolean areKeysEqual(OmKeyInfo oldKey, OmKeyInfo newKey) {
    if (oldKey == null && newKey == null) return true;
    if (oldKey != null) {
      return oldKey.equals(newKey);
    }
    return false;
  }

  private ClosableIterator<String> getsKeysToCheck(final String oldSnapshot,
                                                   final String newSnapshot)
      throws RocksDBException {
    final List<LiveFileMetaData> oldSsSstFiles = snapshotDBHandler
        .getKeyTableSSTFiles(oldSnapshot);
    final List<LiveFileMetaData> newSsSstFiles = snapshotDBHandler
        .getKeyTableSSTFiles(newSnapshot);

    // Ignore same SST files.
    final List<LiveFileMetaData> identicalFiles = SSTFileComparatorFactory
        .getFileNameBasedComparator()
        .getIdenticalFiles(oldSsSstFiles, newSsSstFiles);


    // Filter files based on Compaction Aware SSTFile Comparator.
    final List<LiveFileMetaData> filesWithSameKeys = SSTFileComparatorFactory
        .getCompactionAwareComparator()
        .getIdenticalFiles(oldSsSstFiles, newSsSstFiles);

    final Set<LiveFileMetaData> filesToIgnore = new HashSet<>();
    filesToIgnore.addAll(identicalFiles);
    filesToIgnore.addAll(filesWithSameKeys);
    final Set<LiveFileMetaData> filteredOldSstFiles = oldSsSstFiles.stream()
        .parallel().filter(file -> !filesToIgnore.contains(file))
        .collect(Collectors.toSet());

    final Set<LiveFileMetaData> filteredNewSstFiles = newSsSstFiles.stream()
        .parallel().filter(file -> !filesToIgnore.contains(file))
        .collect(Collectors.toSet());

    final Set<LiveFileMetaData> filteredFiles = new HashSet<>();
    filteredFiles.addAll(filteredOldSstFiles);
    filteredFiles.addAll(filteredNewSstFiles);

    OMSSTFileReader sstFileReader = new OMSSTFileReader(filteredFiles);
    return sstFileReader.getKeyIterator();
  }

}
