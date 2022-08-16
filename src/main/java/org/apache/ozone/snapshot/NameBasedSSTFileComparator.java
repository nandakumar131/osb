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

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class NameBasedSSTFileComparator implements SSTFileComparator {

  @Override
  public List<LiveFileMetaData> getIdenticalFiles(
      final List<LiveFileMetaData> oldSnapshotFiles,
      final List<LiveFileMetaData> newSnapshotFiles) {

    //TODO: Add additional checks later, for now we only check the file name.
    final Set<String> oldSstFileNames = oldSnapshotFiles.stream().parallel()
        .map(LiveFileMetaData::fileName).collect(Collectors.toSet());
    return newSnapshotFiles.stream().parallel().filter(file -> oldSstFileNames.contains(file.fileName()))
        .collect(Collectors.toList());
  }
}
