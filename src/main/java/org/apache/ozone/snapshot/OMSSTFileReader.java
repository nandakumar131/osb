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

import java.io.Closeable;
import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;
import java.util.NoSuchElementException;

public class OMSSTFileReader {

    private final Collection<LiveFileMetaData> sstFiles;
    public OMSSTFileReader(Collection<LiveFileMetaData> sstFiles) {
        this.sstFiles = sstFiles;
    }

    public ClosableIterator<String> getKeyIterator() throws RocksDBException {
        return new OMSSTFileIterator();
    }

    public interface ClosableIterator<T> extends Iterator<T>, Closeable {}

    private class OMSSTFileIterator implements ClosableIterator<String> {

        private final Iterator<LiveFileMetaData> fileNameIterator;

        private final Options options;
        private final ReadOptions readOptions;
        private LiveFileMetaData currentFile;
        private SstFileReader currentFileReader;
        private SstFileReaderIterator currentFileIterator;

        private OMSSTFileIterator() throws RocksDBException {
            this.options = new Options();
            this.readOptions = new ReadOptions();
            this.fileNameIterator = sstFiles.iterator();
            moveToNextFile();
        }

        @Override
        public boolean hasNext() {
            try {
                do {
                    if (currentFileIterator.isValid()) {
                        return true;
                    }
                } while (moveToNextFile());
            } catch (RocksDBException e) {
                return false;
            }
            return false;
        }

        @Override
        public String next() {
            if (hasNext()) {
                final String value = new String(currentFileIterator.key());
                currentFileIterator.next();
                return value;
            }
            throw new NoSuchElementException("No more keys");
        }

        @Override
        public void close() throws IOException {
            closeCurrentFile();
        }

        private boolean moveToNextFile() throws RocksDBException {
            if (fileNameIterator.hasNext()) {
                closeCurrentFile();
                currentFile = fileNameIterator.next();
                currentFileReader = new SstFileReader(options);
                final String file = currentFile.path() + currentFile.fileName();
                currentFileReader.open(file);
                currentFileIterator = currentFileReader.newIterator(readOptions);
                currentFileIterator.seekToFirst();
                return true;
            }
            return false;
        }

        private void closeCurrentFile() {
            if (currentFile != null) {
                currentFileIterator.close();
                currentFileReader.close();
                currentFile = null;
            }
        }
    }

}
