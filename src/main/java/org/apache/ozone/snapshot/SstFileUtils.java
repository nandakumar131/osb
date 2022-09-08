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

import org.rocksdb.LiveFileMetaData;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class SstFileUtils {

  /**
   * Filters out similar files and returns only the files that are different.
   * @param oldSnapshotFiles
   * @param newSnapshotFiles
   * @return new files created after the creation of old snapshot.
   */
  public static Set<LiveFileMetaData> getDeltaFiles(
      List<LiveFileMetaData> oldSnapshotFiles,
      List<LiveFileMetaData> newSnapshotFiles) {
    // Ignore same SST files.
    final List<LiveFileMetaData> identicalFiles = SSTFileComparatorFactory
        .getFileNameBasedComparator()
        .getIdenticalFiles(oldSnapshotFiles, newSnapshotFiles);


    // Filter files based on Compaction Aware SSTFile Comparator.
    final List<LiveFileMetaData> filesWithSameKeys = SSTFileComparatorFactory
        .getCompactionAwareComparator()
        .getIdenticalFiles(oldSnapshotFiles, newSnapshotFiles);

    final Set<LiveFileMetaData> filesToIgnore = new HashSet<>();
    filesToIgnore.addAll(identicalFiles);
    filesToIgnore.addAll(filesWithSameKeys);
    System.out.println("No of SST Files Eliminated due to same name: " + identicalFiles.size());
    System.out.println("No of SST Files Eliminated due to compaction aware filter: " + filesWithSameKeys.size());
    final Set<LiveFileMetaData> filteredOldSstFiles = oldSnapshotFiles.stream()
        .parallel().filter(file -> !filesToIgnore.contains(file))
        .collect(Collectors.toSet());

    final Set<LiveFileMetaData> filteredNewSstFiles = newSnapshotFiles.stream()
        .parallel().filter(file -> !filesToIgnore.contains(file))
        .collect(Collectors.toSet());

    final Set<LiveFileMetaData> filteredFiles = new HashSet<>();
    filteredFiles.addAll(filteredOldSstFiles);
    filteredFiles.addAll(filteredNewSstFiles);

    return filteredFiles;
  }
}
