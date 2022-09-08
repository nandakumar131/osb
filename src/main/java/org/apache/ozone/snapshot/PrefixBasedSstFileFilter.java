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
import org.rocksdb.Options;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDBException;
import org.rocksdb.SstFileReader;
import org.rocksdb.SstFileReaderIterator;

import java.util.HashSet;
import java.util.Set;

public class PrefixBasedSstFileFilter extends SstFileFilter {

  private String prefix;

  PrefixBasedSstFileFilter(String prefix){
    this.prefix = prefix;
  }

  public Set<LiveFileMetaData> filter(Set<LiveFileMetaData> inputFiles,
      String prefix) {
    Set<LiveFileMetaData> result = new HashSet<>();
    for (LiveFileMetaData fileMetaData : inputFiles) {
      String firstDbKey = getDbKey(new String(fileMetaData.smallestKey()));
      String lastDbKey = getDbKey(new String(fileMetaData.largestKey()));
      String dbPath = fileMetaData.path()  + fileMetaData.fileName();


      if (firstDbKey.compareTo(prefix) <= 0
          && prefix.compareTo(lastDbKey) <= 0) {
        // Every Sst File contains metadata block which contains info about smallest
        // and largest key (Lexicographically ordered by default)
        // The above 'if' condition checks whether the given prefix belongs to this
        // range , however there might by false positives.
        // For example for key prefix= "vol5/buck3"
        // SST file with range [vol10/buck1, vol9/buck5] will return true,
        // however this key might have been created after the sst file was created.
        // In order to rule out those FPs we open the file and seek to the particular prefix.
        if (sstFileContainsKeyWithPrefix(fileMetaData.fileName(), prefix,
            dbPath))
          result.add(fileMetaData);
      }
    }

    return result;
  }

  private boolean sstFileContainsKeyWithPrefix(String fileName,
      String prefix,String sstFilePath) {
    try (SstFileReader sstFileReader = new SstFileReader(new Options())) {
      sstFileReader.open(sstFilePath);
      SstFileReaderIterator
          iterator = sstFileReader.newIterator(new ReadOptions());
      iterator.seek(prefix.getBytes());
      String seekKey = new String(iterator.key());
      if(seekKeyMatchesPrefix(seekKey,prefix)){
        return true;
      }
    } catch (RocksDBException e) {
      e.printStackTrace();
    }
    return false;
  }

  private static boolean seekKeyMatchesPrefix(String seekKey, String prefix) {
    return getDbKey(seekKey).equals(prefix);
  }

  private static String getDbKey(String s) {
    if(!s.startsWith("/")) {
      s = "/".concat(s);
    }
    String[] components = s.split("/");
    return "/" + components[1] + "/" + components[2] + "/" ;
  }



  @Override
  Set<LiveFileMetaData> filter(Set<LiveFileMetaData> inputFiles) {
    return filter(inputFiles,prefix);
  }
}
