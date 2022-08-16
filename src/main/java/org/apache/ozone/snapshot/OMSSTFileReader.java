package org.apache.ozone.snapshot;

import org.rocksdb.*;

import java.util.HashSet;
import java.util.Set;

public class OMSSTFileReader {
    public static Set<String> getKeys(final LiveFileMetaData fileMetaData) throws RocksDBException {
        final Set<String> keys = new HashSet<>();
        final SstFileReader sstFileReader = new SstFileReader(new Options());
        final String file = fileMetaData.path() + fileMetaData.fileName();
        sstFileReader.open(file);
        SstFileReaderIterator iterator = sstFileReader.newIterator(new ReadOptions());
        iterator.seekToFirst();
        while (iterator.isValid()) {
            keys.add(new String(iterator.key()));
        }
        iterator.close();
        sstFileReader.close();
        return keys;
    }

}
